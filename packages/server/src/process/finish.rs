use {
	crate::{Context, Server},
	bytes::Bytes,
	futures::{StreamExt as _, TryFutureExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_either::Either,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) async fn finish_process_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self.get_remote_client(remote).await?;
			let arg = tg::process::finish::Arg {
				checksum: arg.checksum,
				error: arg.error,
				exit: arg.exit,
				local: None,
				output: arg.output,
				remotes: None,
			};
			client.finish_process(id, arg).await?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// If the task for the process is not the current task, then abort it.
		if self
			.process_tasks
			.get_task_id(id)
			.is_some_and(|task_id| task_id != tokio::task::id())
		{
			self.process_tasks.abort(id);
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
		#[derive(Clone, db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
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
							local: Some(true),
							remotes: None,
							token,
						};
						self.cancel_process(&id, arg).await.ok();
					}
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		// Verify the checksum if one was provided.
		if let Some(expected) = &data.expected_checksum
			&& exit == 0
		{
			let actual = arg
				.checksum
				.as_ref()
				.ok_or_else(|| tg::error!(%id, "the actual checksum was not set"))?;
			if expected != actual {
				let data = tg::error::Data {
					code: Some(tg::error::Code::ChecksumMismatch),
					message: Some("checksum mismatch".into()),
					values: [
						("expected".into(), expected.to_string()),
						("actual".into(), actual.to_string()),
					]
					.into(),
					..Default::default()
				};
				error = Some(Either::Left(data));
				exit = 1;
			}
		}

		if !self.config.advanced.internal_error_locations
			&& let Some(Either::Left(error)) = &mut error
		{
			error.remove_internal_locations();
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
					depth = null,
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
		let error_data_or_id = error.as_ref().map(|error| match error {
			Either::Left(data) => serde_json::to_string(data).unwrap(),
			Either::Right(id) => id.to_string(),
		});
		let error_code = error.as_ref().and_then(|error| match error {
			Either::Left(data) => data.code.map(|code| code.to_string()),
			Either::Right(_) => None,
		});
		let params = db::params![
			arg.checksum.as_ref().map(ToString::to_string),
			error_data_or_id,
			error_code,
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

		// Update token count to 0.
		let statement = formatdoc!(
			"
				update processes
				set token_count = 0
				where id = {p}1;
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
		let errors = data.error.as_ref().into_iter().flat_map(|e| match e {
			Either::Left(data) => {
				let mut children = BTreeSet::new();
				data.children(&mut children);
				children
					.into_iter()
					.map(|object| {
						let kind = crate::index::message::ProcessObjectKind::Error;
						(object, kind)
					})
					.collect::<Vec<_>>()
			},
			Either::Right(id) => {
				let id = id.clone().into();
				let kind = crate::index::message::ProcessObjectKind::Error;
				vec![(id, kind)]
			},
		});
		let log = data.log.as_ref().map(|id| {
			let id = id.clone().into();
			let kind = crate::index::message::ProcessObjectKind::Log;
			(id, kind)
		});
		let mut outputs = BTreeSet::new();
		if let Some(output) = &output {
			output.children(&mut outputs);
		}
		let outputs = outputs.into_iter().map(|object| {
			let kind = crate::index::message::ProcessObjectKind::Output;
			(object, kind)
		});
		let objects = std::iter::once(command)
			.chain(errors)
			.chain(log)
			.chain(outputs)
			.collect();
		let children = children.into_iter().map(|row| row.child).collect();
		let message = crate::index::Message::PutProcess(crate::index::message::PutProcess {
			children,
			stored: crate::process::stored::Output::default(),
			id: id.clone(),
			metadata: tg::process::Metadata::default(),
			objects,
			touched_at: now,
		});
		let message = message.serialize()?;
		self.tasks
			.spawn(|_| {
				let server = self.clone();
				async move {
					let result = server
						.messenger
						.stream_publish("index".to_owned(), message)
						.map_err(|source| tg::error!(!source, "failed to publish the message"))
						.and_then(|future| {
							future.map_err(|source| {
								tg::error!(!source, "failed to publish the message")
							})
						})
						.await;
					if let Err(error) = result {
						tracing::error!(?error, "failed to publish the put process index message");
					}
				}
			})
			.detach();

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

	pub(crate) async fn handle_finish_process_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id.parse()?;
		let arg = request.json().await?;
		self.finish_process_with_context(context, &id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
