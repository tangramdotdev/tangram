use crate::Server;
use bytes::Bytes;
use futures::{StreamExt as _, stream::FuturesUnordered};
use indoc::formatdoc;
use std::collections::BTreeSet;
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

		// If the task for the process is not the current task, then abort it.
		if self
			.process_task_map
			.get_task_id(id)
			.is_some_and(|task_id| task_id != tokio::task::id())
		{
			self.process_task_map.abort(id);
		}

		let tg::process::finish::Arg {
			mut error,
			output,
			mut exit,
			..
		} = arg;

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
		#[derive(Clone, serde::Deserialize)]
		struct Row {
			child: tg::process::Id,
			token: Option<String>,
		}
		let statement = formatdoc!(
			"
				select child, token
				from process_children
				where process = {p}1
				order by position;
			"
		);
		let params = db::params![id.to_string()];
		let children = connection
			.query_all_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.into_iter()
			.map(|row| row.0)
			.collect::<Vec<_>>();
		drop(connection);

		// Cancel the children.
		children
			.clone()
			.into_iter()
			.map(|row| {
				let id = row.child;
				let token = row.token;
				async move {
					if let Some(token) = token {
						let arg = tg::process::cancel::Arg {
							token,
							remote: None,
						};
						self.cancel_process(&id, arg).await.ok();
					}
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		// Verify the checksum if one was provided.
		if let Some(expected) = &data.expected_checksum {
			if exit == 0 {
				let actual = arg
					.checksum
					.as_ref()
					.ok_or_else(|| tg::error!(%id, "the actual checksum was not set"))?;
				if expected != actual {
					error = Some(
						tg::error!(
							code = tg::error::Code::ChecksumMismatch,
							%expected,
							%actual,
							"checksum mismatch",
						)
						.to_data(),
					);
					exit = 1;
				}
			}
		}

		// Remove internal error locations if necessary.
		if !self.config.advanced.internal_error_locations {
			if let Some(error) = &mut error {
				let mut stack = vec![error];
				while let Some(error) = stack.pop() {
					if let Some(location) = &mut error.location {
						if matches!(location.file, tg::error::data::File::Internal(_)) {
							error.location = None;
						}
					}
					if let Some(stack) = &mut error.stack {
						stack.retain(|location| {
							!matches!(location.file, tg::error::data::File::Internal(_))
						});
					}
					if let Some(source) = &mut error.source {
						stack.push(&mut *source.item);
					}
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
					error_code = {p}3,
					finished_at = {p}4,
					heartbeat_at = null,
					output = {p}5,
					exit = {p}6,
					status = {p}7,
					touched_at = {p}8
				where
					id = {p}9 and
					status != 'finished';
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![
			arg.checksum.as_ref().map(ToString::to_string),
			error.clone().map(db::value::Json),
			error
				.as_ref()
				.and_then(|error| error.code.map(|code| code.to_string())),
			now,
			output.clone().map(db::value::Json),
			exit,
			tg::process::Status::Finished.to_string(),
			now,
			id.to_string(),
		];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n != 1 {
			return Err(tg::error!("the process was already finished"));
		}

		// Delete the tokens.
		let statement = formatdoc!(
			"
				delete from process_tokens where process = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Publish the put process index message.
		let command = (
			data.command.clone().into(),
			crate::index::message::ProcessObjectKind::Command,
		);
		let mut outputs = BTreeSet::new();
		if let Some(output) = &output {
			output.children(&mut outputs);
		}
		let outputs = outputs
			.into_iter()
			.map(|object| (object, crate::index::message::ProcessObjectKind::Output));
		let objects = std::iter::once(command).chain(outputs).collect();
		let children = children.into_iter().map(|row| row.child).collect();
		let message = crate::index::Message::PutProcess(crate::index::message::PutProcess {
			children,
			complete: crate::process::complete::Output::default(),
			id: id.clone(),
			metadata: tg::process::Metadata::default(),
			objects,
			touched_at: now,
		});
		let message = message.serialize()?;
		let _published = self
			.messenger
			.stream_publish("index".to_owned(), message)
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
