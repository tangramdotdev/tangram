use crate::Server;
use bytes::Bytes;
use futures::{TryStreamExt as _, stream::FuturesUnordered};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::prelude::*;

impl Server {
	pub async fn finish_process(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::finish::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let client = self.get_remote_client(remote.clone()).await?;
			let arg = tg::process::finish::Arg {
				remote: None,
				..arg.clone()
			};
			client.finish_process(id, arg).await?;
			return Ok(());
		}

		// If the process is not the current process then abort it.
		let process_task_id = self.processes.get_task_id(id);
		let should_abort = if let Some(process_task_id) = process_task_id {
			process_task_id != tokio::task::id()
		} else {
			true
		};
		if should_abort {
			self.processes.abort(id);
		}

		let tg::process::finish::Arg {
			mut error,
			output,
			mut exit,
			..
		} = arg;

		// Attempt to set the process's status to finishing.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				update processes
				set status = 'finishing'
				where id = {p}1 and case
					when {p}2 then status = 'started' or status = 'finishing'
					else status = 'started'
				end;
			"
		);
		let params = db::params![id, arg.force];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);
		if n == 0 {
			return Err(tg::error!(%id, "failed to find the process"));
		}

		// Get the process.
		let Some(tg::process::get::Output { data, .. }) = self.try_get_process_local(id).await?
		else {
			return Err(tg::error!("failed to find the process"));
		};

		// Get the process's children.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select child
				from process_children
				where process = {p}1
				order by position;
			"
		);
		let params = db::params![id];
		let children = connection
			.query_all_value_into::<tg::process::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);

		// Cancel unfinished children.
		children
			.clone()
			.into_iter()
			.map(|child| async move {
				let error = tg::error!(
					code = tg::error::Code::Cancelation,
					"the parent was finished"
				);
				let arg = tg::process::finish::Arg {
					checksum: None,
					error: Some(error),
					exit: 1,
					force: false,
					output: None,
					remote: None,
				};
				self.finish_process(&child, arg).await.ok();
				Ok::<_, tg::Error>(())
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		// Verify the checksum if one was provided.
		if let Some(expected) = &data.expected_checksum {
			if exit == 0 {
				let actual = arg
					.checksum
					.as_ref()
					.ok_or_else(|| tg::error!("the actual checksum was not set"))?;
				if expected != actual {
					error = Some(tg::error!(
						"checksum does not match, expected {expected}, actual {actual}"
					));
					exit = 1;
				}
			}
		}

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Update the process.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update processes
				set
					actual_checksum = {p}1,
					error = {p}2,
					finished_at = {p}3,
					heartbeat_at = null,
					output = {p}4,
					exit = {p}5,
					status = {p}6,
					touched_at = {p}7
				where id = {p}8;
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![
			arg.checksum,
			error.map(db::value::Json),
			now,
			output.clone().map(db::value::Json),
			exit,
			tg::process::Status::Finished,
			now,
			id,
		];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the command from the database.
		let statement = format!(
			"
				select command
				from processes
				where id = {p}1
			",
		);
		let params = db::params![id];
		let command: Option<tg::object::Id> = connection
			.query_optional_value_into::<tg::object::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Get the children from the database.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select child
				from process_children
				where process = {p}1
				order by position;
			"
		);
		let params = db::params![id,];
		let children = connection
			.query_all_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Publish the put process message.
		let outputs = output
			.as_ref()
			.map(tg::value::Data::children)
			.into_iter()
			.flatten();
		let outputs = outputs.map(|output| (output, crate::index::ProcessObjectKind::Output));
		let command = command
			.map(|command| (command, crate::index::ProcessObjectKind::Command))
			.into_iter();
		let objects = outputs.chain(command).collect();
		let message = crate::index::Message::PutProcess(crate::index::PutProcessMessage {
			id: id.clone(),
			touched_at: now,
			children: Some(children),
			objects,
		});
		let message = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		let _published = self
			.messenger
			.stream_publish("index".to_owned(), message.into())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		// Publish the status message.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				server
					.messenger
					.publish(format!("processes.{id}.status"), Bytes::new())
					.await
					.inspect_err(|error| tracing::error!(%error, %id, "failed to publish"))
					.ok();
			}
		});

		Ok(())
	}

	pub(crate) async fn handle_finish_process_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		handle.finish_process(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
