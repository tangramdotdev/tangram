use {
	crate::{Context, Server},
	futures::{StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) async fn try_finish_process_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<Option<()>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let location = self.location_with_regions(arg.location.as_ref())?;

		let output = match location {
			crate::location::Location::Local { region: None } => {
				self.try_finish_process_local(id, arg).await?
			},
			crate::location::Location::Local {
				region: Some(region),
			} => self.try_finish_process_region(id, arg, region).await?,
			crate::location::Location::Remote { remote, region } => {
				self.try_finish_process_remote(id, arg, remote, region)
					.await?
			},
		};

		Ok(output)
	}

	async fn try_finish_process_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<Option<()>> {
		let tg::process::finish::Arg {
			mut error,
			output,
			mut exit,
			..
		} = arg;

		// Get the process.
		let Some(tg::process::get::Output { data, .. }) = self
			.try_get_process_local(id, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?
		else {
			return Ok(None);
		};

		// Get the process's children.
		let connection =
			self.sandbox_store.connection().await.map_err(|source| {
				tg::error!(!source, "failed to get a sandbox store connection")
			})?;
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
							location: Some(tg::location::Location::Local(
								tg::location::Local::default(),
							)),
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
				error = Some(tg::Either::Left(data));
				exit = 1;
			}
		}

		if !self.config.advanced.internal_error_locations
			&& let Some(tg::Either::Left(error)) = &mut error
		{
			error.remove_internal_locations();
		}

		// Get a sandbox store connection.
		let mut connection = self
			.sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "faile to acquire a transaction"))?;

		// Get the current open stdio flags so the corresponding readers can be woken after commit.
		let p = transaction.p();
		#[derive(db::row::Deserialize)]
		struct OpenRow {
			stderr: Option<bool>,
			stdin: Option<bool>,
			stdout: Option<bool>,
		}
		let statement = formatdoc!(
			"
				select stderr_open as stderr, stdin_open as stdin, stdout_open as stdout
				from processes
				where id = {p}1 and status != 'finished';
			"
		);
		let params = db::params![id.to_string()];
		let open = transaction
			.query_optional_into::<OpenRow>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Update the process.
		let statement = formatdoc!(
			"
				update processes
				set
					actual_checksum = {p}1,
					depth = null,
					error = {p}2,
					error_code = {p}3,
					finished_at = {p}4,
					output = {p}5,
					exit = {p}6,
					status = {p}7,
					stderr_open = case when stderr_open is null then null else false end,
					stdin_open = case when stdin_open is null then null else false end,
					stdout_open = case when stdout_open is null then null else false end,
					touched_at = {p}8
				where
					id = {p}9 and
					status != 'finished';
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let error_data_or_id = error.as_ref().map(|error| match error {
			tg::Either::Left(data) => serde_json::to_string(data).unwrap(),
			tg::Either::Right(id) => id.to_string(),
		});
		let error_code = error.as_ref().and_then(|error| match error {
			tg::Either::Left(data) => data.code.map(|code| code.to_string()),
			tg::Either::Right(_) => None,
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
		let n = transaction
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
		transaction
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
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Enqueue the process for finalization.
		let statement = formatdoc!(
			"
				insert into process_finalize_queue (created_at, process, status)
				values ({p}1, {p}2, {p}3);
			"
		);
		let params = db::params![now, id.to_string(), "created"];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		drop(connection);

		// Publish the finalize message.
		self.spawn_publish_process_finalize_message_task();

		if open.as_ref().is_some_and(|row| row.stdin == Some(true)) {
			self.spawn_publish_process_stdio_close_message_task(
				id,
				tg::process::stdio::Stream::Stdin,
			);
		}
		if open.as_ref().is_some_and(|row| row.stdout == Some(true)) {
			self.spawn_publish_process_stdio_close_message_task(
				id,
				tg::process::stdio::Stream::Stdout,
			);
		}
		if open.as_ref().is_some_and(|row| row.stderr == Some(true)) {
			self.spawn_publish_process_stdio_close_message_task(
				id,
				tg::process::stdio::Stream::Stderr,
			);
		}

		// Publish the status.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				server
					.messenger
					.publish(format!("processes.{id}.status"), ())
					.await
					.inspect_err(|error| tracing::error!(%error, %id, "failed to publish"))
					.ok();
			}
		});

		Ok(Some(()))
	}

	async fn try_finish_process_region(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
		region: String,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_client(region.clone()).await.map_err(
			|source| tg::error!(!source, region = %region, %id, "failed to get the region client"),
		)?;
		let arg = tg::process::finish::Arg {
			checksum: arg.checksum,
			error: arg.error,
			exit: arg.exit,
			location: Some(tg::location::Location::Local(tg::location::Local {
				regions: Some(vec![region.clone()]),
			})),
			output: arg.output,
		};
		match client.try_finish_process(id, arg).await {
			Ok(Some(())) => Ok(Some(())),
			Ok(None) => Ok(None),
			Err(error) => {
				let output = client
					.try_get_process(id, tg::process::get::Arg::default())
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							%id,
							"failed to confirm the process state after the finish request failed"
						)
					})?;
				if output.is_none_or(|output| !output.data.status.is_finished()) {
					return Err(
						tg::error!(!error, region = %region, %id, "failed to finish the process"),
					);
				}
				Ok(Some(()))
			},
		}
	}

	async fn try_finish_process_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_client(remote.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote, %id, "failed to get the remote client"),
		)?;
		let arg = tg::process::finish::Arg {
			checksum: arg.checksum,
			error: arg.error,
			exit: arg.exit,
			location: Some(tg::location::Location::Local(tg::location::Local {
				regions: region.map(|region| vec![region]),
			})),
			output: arg.output,
		};
		match client.try_finish_process(id, arg).await {
			Ok(Some(())) => Ok(Some(())),
			Ok(None) => Ok(None),
			Err(error) => {
				let output = client
					.try_get_process(id, tg::process::get::Arg::default())
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							%id,
							"failed to confirm the process state after the finish request failed"
						)
					})?;
				if output.is_none_or(|output| !output.data.status.is_finished()) {
					return Err(
						tg::error!(!error, remote = %remote, %id, "failed to finish the process"),
					);
				}
				Ok(Some(()))
			},
		}
	}

	pub(crate) async fn handle_finish_process_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Finish the process.
		let Some(()) = self
			.try_finish_process_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to finish the process"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
