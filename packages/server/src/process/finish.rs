use crate::Server;
use bytes::Bytes;
use futures::{StreamExt as _, stream::FuturesUnordered};
use indoc::formatdoc;
use std::path::PathBuf;
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
			path: Option<PathBuf>,
			tag: Option<tg::Tag>,
			token: Option<String>,
		}
		let statement = formatdoc!(
			"
				select child, path, tag, token
				from process_children
				where process = {p}1
				order by position;
			"
		);
		let params = db::params![id];
		let children = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
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
						tg::error!("checksum does not match, expected {expected}, actual {actual}")
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
					finished_at = {p}3,
					heartbeat_at = null,
					output = {p}4,
					exit = {p}5,
					status = {p}6,
					touched_at = {p}7
				where
					id = {p}8 and
					status != 'finished';
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![
			arg.checksum,
			error.clone().map(db::value::Json),
			now,
			output.clone().map(db::value::Json),
			exit,
			tg::process::Status::Finished,
			now,
			id,
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
		let params = db::params![id];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Publish the put process message.
		let objects = std::iter::once((
			data.command.clone().into(),
			crate::index::ProcessObjectKind::Command,
		))
		.chain(
			error
				.as_ref()
				.map(tg::error::Data::children)
				.into_iter()
				.flatten()
				.map(|object| (object, crate::index::ProcessObjectKind::Error)),
		)
		.chain(
			output
				.as_ref()
				.map(tg::value::Data::children)
				.into_iter()
				.flatten()
				.map(|object| (object, crate::index::ProcessObjectKind::Output)),
		)
		.collect();
		let children = children
			.into_iter()
			.map(|row| tg::Referent {
				item: row.child,
				options: tg::referent::Options {
					path: row.path,
					tag: row.tag,
				},
			})
			.collect();
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
