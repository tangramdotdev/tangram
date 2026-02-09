use {
	crate::{Context, ProcessPermit, Server, database},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream, FuturesUnordered},
	},
	indoc::{formatdoc, indoc},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::prelude::*,
};

#[derive(derive_more::Debug)]
struct LocalOutput {
	id: tg::process::Id,
	#[debug(ignore)]
	permit: Option<ProcessPermit>,
	status: tg::process::Status,
	token: Option<String>,
	wait: Option<tg::process::wait::Output>,
}

impl Server {
	pub async fn try_spawn_process_with_context(
		&self,
		context: &Context,
		mut arg: tg::process::spawn::Arg,
	) -> tg::Result<
		BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>,
	> {
		// If the process context is set, update the parent, remotes, and retry.
		if let Some(process) = &context.process {
			arg.parent = Some(process.id.clone());
			if let Some(remote) = &process.remote {
				arg.remotes = Some(vec![remote.clone()]);
			}
			arg.retry = process.retry;
		}

		// Create the progress.
		let progress = crate::progress::Handle::new();

		// Spawn the task.
		let task = Task::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move |_| match Box::pin(server.try_spawn_process_task(arg, &progress)).await {
				Ok(output) => {
					progress.output(output);
				},
				Err(error) => {
					progress.error(error);
					progress.output(None);
				},
			}
		});

		Ok(progress.stream().attach(task).boxed())
	}

	async fn try_spawn_process_task(
		&self,
		arg: tg::process::spawn::Arg,
		progress: &crate::progress::Handle<Option<tg::process::spawn::Output>>,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		// Forward to remote if requested.
		if let Some(name) = Self::remote(arg.local, arg.remotes.as_ref())? {
			// Push the command.
			let push_arg = tg::push::Arg {
				commands: true,
				items: vec![tg::Either::Left(arg.command.item().clone().into())],
				remote: Some(name.clone()),
				..Default::default()
			};
			let stream = self
				.push(push_arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to push the command"))?;
			let mut stream = pin!(stream);
			while let Some(event) = stream.try_next().await? {
				if event.is_output() {
					break;
				}
				progress.forward(Ok(event));
			}

			let remote = self
				.get_remote_client(name.clone())
				.await
				.map_err(|source| tg::error!(!source, %name, "failed to get the remote client"))?;

			// Spawn the process.
			let arg = tg::process::spawn::Arg {
				local: None,
				remotes: None,
				..arg
			};
			let stream = remote.try_spawn_process(arg).await.map_err(
				|source| tg::error!(!source, %name, "failed to spawn process on remote"),
			)?;
			let mut stream = pin!(stream);

			// Forward any events.
			while let Some(event) = stream.next().await {
				if let Some(output) = progress.forward(event) {
					return Ok(output);
				}
			}
			return Err(tg::error!("expected an output"));
		}

		// Guard against concurrent cleans.
		let _clean_guard = self.try_acquire_clean_guard()?;

		// Get the host.
		let command_ = tg::Command::with_id(arg.command.item.clone());
		let host = command_.host(self).await.ok();

		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Determine if the process is cacheable.
		let cacheable = arg.checksum.is_some()
			|| (arg.mounts.is_empty()
				&& !arg.network
				&& arg.stdin.is_none()
				&& arg.stdout.is_none()
				&& arg.stderr.is_none());

		// Get or create a local process.
		let mut output = if cacheable
			&& matches!(arg.cached, None | Some(true))
			&& let Some(output) = self
				.try_get_cached_process_local(&transaction, &arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to get a cached local process"))?
		{
			tracing::trace!(?output, "got cached local process");
			Some(output)
		} else if cacheable
			&& matches!(arg.cached, None | Some(true))
			&& let Some(host) = &host
			&& let Some(output) = self
				.try_get_cached_process_with_mismatched_checksum_local(&transaction, &arg, host)
				.await
				.map_err(|source| {
					tg::error!(
						!source,
						"failed to get a cached local process with mismatched checksum"
					)
				})? {
			tracing::trace!(?output, "got cached local process with mismatched checksum");
			Some(output)
		} else if matches!(arg.cached, None | Some(false)) {
			let host = host.ok_or_else(|| tg::error!("expected the host to be set"))?;
			let output = self
				.create_local_process(&transaction, &arg, cacheable, &host)
				.await
				.map_err(|source| tg::error!(!source, "failed to create a local process"))?;
			tracing::trace!(?output, "created local process");
			Some(output)
		} else {
			None
		};

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Drop the connection.
		drop(connection);

		// If a permit has been acquired, then spawn the process task. Otherwise, enqueue the process and create a task to spawn the process task when the parent's permit is acquired.
		if let Some(output) = &mut output {
			if let Some(permit) = output.permit.take() {
				let process = tg::Process::new(output.id.clone(), None, None, None, None);
				let clean_guard = self.try_acquire_clean_guard()?;
				self.spawn_process_task(&process, permit, clean_guard);
			} else {
				let payload = crate::process::queue::Message {
					id: output.id.clone(),
				};
				self.messenger
					.stream_publish("queue".into(), payload)
					.await
					.map_err(|source| tg::error!(!source, "failed to enqueue the process"))?
					.await
					.map_err(|source| tg::error!(!source, "failed to enqueue the process"))?;
				self.spawn_process_parent_permit_task(&arg, &output.id);
			}
		}

		// Determine if the local process is finished.
		let finished = output
			.as_ref()
			.is_some_and(|output| output.status.is_finished());

		// Create a future that will await the local process if there is one.
		let local_future = {
			let id = output.as_ref().map(|output| output.id.clone());
			let wait = output.as_ref().and_then(|output| output.wait.clone());
			async {
				if finished {
					return Ok::<_, tg::Error>(wait);
				}
				if let Some(id) = id {
					let wait = self
						.wait_process(&id, tg::process::wait::Arg::default())
						.await
						.map_err(
							|source| tg::error!(!source, %id, "failed to wait for the process"),
						)?;
					Ok(Some(wait))
				} else {
					Ok(None)
				}
			}
		};

		// Create a future that will attempt to get a cached remote process if possible.
		let remote_future = {
			async {
				if finished {
					return Ok::<_, tg::Error>(None);
				}
				if cacheable && matches!(arg.cached, None | Some(true)) {
					let Some(output) =
						self.try_get_cached_process_remote(&arg)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to get a cached remote process")
							})?
					else {
						return Ok(None);
					};
					Ok(Some(output))
				} else {
					Ok(None)
				}
			}
		};

		// If the local process finishes before the remote responds, then use the local process. If a remote process is found sooner, then spawn a task to cancel the local process and use the remote process.
		let output = match future::select(pin!(local_future), pin!(remote_future)).await {
			future::Either::Left((result, remote_future)) => {
				if let Some(wait) = result? {
					let output = output.unwrap();
					tg::process::spawn::Output {
						process: output.id,
						remote: None,
						token: output.token,
						wait: Some(wait),
					}
				} else {
					let Some(remote_output) = remote_future.await? else {
						return Ok(None);
					};
					remote_output
				}
			},
			future::Either::Right((result, _)) => {
				if let Ok(Some(remote_output)) = result {
					if let Some(output) = output
						&& let Some(token) = output.token
					{
						tokio::spawn({
							let server = self.clone();
							async move {
								let arg = tg::process::cancel::Arg {
									local: Some(true),
									remotes: None,
									token,
								};
								server.cancel_process(&output.id, arg).boxed().await.ok();
							}
						});
					}
					remote_output
				} else {
					let Some(output) = output else {
						return Ok(None);
					};
					tg::process::spawn::Output {
						process: output.id,
						remote: None,
						token: output.token,
						wait: output.wait,
					}
				}
			},
		};

		if output.remote.is_none()
			&& let Some(parent) = &arg.parent
		{
			self.add_process_child(
				parent,
				&output.process,
				&arg.command.options,
				output.token.as_ref(),
			)
			.await
			.map_err(
				|source| tg::error!(!source, %parent, child = %output.process, "failed to add the process as a child"),
			)?;
		}

		Ok(Some(output))
	}

	async fn try_get_cached_process_local(
		&self,
		transaction: &database::Transaction<'_>,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<Option<LocalOutput>> {
		let p = transaction.p();

		// Attempt to get a matching process.
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::process::Id,
			error: Option<String>,
			exit: Option<u8>,
			#[tangram_database(as = "Option<db::value::Json<tg::value::Data>>")]
			output: Option<tg::value::Data>,
			#[tangram_database(as = "db::value::FromStr")]
			status: tg::process::Status,
		}
		let params = match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => {
				"with params as (select $1::text as command, $2::text as checksum)"
			},
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(_) => "with params as (select ?1 as command, ?2 as checksum)",
		};
		let is = match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => "is not distinct from",
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(_) => "is",
		};
		let isnt = match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => "is distinct from",
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(_) => "is not",
		};
		let statement = formatdoc!(
			"
				{params}
				select id, error, exit, output, status
				from processes, params
				where
					processes.command = params.command and
					processes.cacheable = true and
					processes.expected_checksum {is} params.checksum and
					processes.error_code {isnt} 'cancellation' and
					processes.error_code {isnt} 'heartbeat_expiration'
				order by processes.created_at desc
				limit 1;
			"
		);
		let params = db::params![
			arg.command.item.to_string(),
			arg.checksum.as_ref().map(ToString::to_string),
		];
		let Some(Row {
			id,
			error,
			exit,
			output,
			status,
		}) = transaction
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		// If the process failed and the retry flag is set, then return.
		let failed = error.is_some() || exit.is_some_and(|exit| exit != 0);
		if failed && arg.retry {
			return Ok(None);
		}

		let wait =
			if status == tg::process::Status::Finished {
				let error =
					error
						.map(|error| {
							if error.starts_with('{') {
								serde_json::from_str(&error).map(tg::Either::Left).map_err(
									|source| tg::error!(!source, "failed to deserialize the error"),
								)
							} else {
								error.parse().map(tg::Either::Right).map_err(|source| {
									tg::error!(!source, "failed to parse the error id")
								})
							}
						})
						.transpose()
						.map_err(|source| tg::error!(!source, "invalid error"))?;
				let exit = exit.ok_or_else(|| tg::error!("expected the exit to be set"))?;
				Some(tg::process::wait::Output {
					error,
					exit,
					output,
				})
			} else {
				None
			};

		// If the process is not finished, then create a process token.
		let token = if status == tg::process::Status::Finished {
			None
		} else {
			let token = Self::create_process_token();
			let statement = formatdoc!(
				"
					insert into process_tokens (process, token)
					values ({p}1, {p}2);
				"
			);
			let params = db::params![id.to_string(), token];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Update token count.
			let statement = formatdoc!(
				"
					update processes
					set token_count = token_count + 1
					where id = {p}1;
				"
			);
			let params = db::params![id.to_string()];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			Some(token)
		};

		Ok(Some(LocalOutput {
			id,
			permit: None,
			status,
			token,
			wait,
		}))
	}

	async fn try_get_cached_process_with_mismatched_checksum_local(
		&self,
		transaction: &database::Transaction<'_>,
		arg: &tg::process::spawn::Arg,
		host: &str,
	) -> tg::Result<Option<LocalOutput>> {
		let p = transaction.p();

		// If the checksum is not set, then return.
		let Some(expected_checksum) = arg.checksum.clone() else {
			return Ok(None);
		};

		// Attempt to get a process.
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			actual_checksum: tg::Checksum,
			#[tangram_database(as = "Option<db::value::Json<tg::value::Data>>")]
			output: Option<tg::value::Data>,
		}
		let params = match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => {
				"with params as (select $1::text as command, $2::text as checksum)"
			},
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(_) => "with params as (select ?1 as command, ?2 as checksum)",
		};
		let is = match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => "is not distinct from",
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(_) => "is",
		};
		let statement = formatdoc!(
			"
				{params}
				select actual_checksum, output
				from processes, params
				where
					processes.command = params.command and
					processes.cacheable = true and
					processes.error_code {is} 'checksum_mismatch' and
					processes.actual_checksum is not null and
					split_part(processes.actual_checksum, ':', 1) = split_part(params.checksum, ':', 1)
				order by processes.created_at desc
				limit 1;
			"
		);
		let params = db::params![arg.command.item.to_string(), expected_checksum.to_string()];
		let Some(Row {
			actual_checksum,
			output,
		}) = transaction
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		// Set the exit, output, and error.
		let (exit, error) = if expected_checksum == actual_checksum {
			(0, None)
		} else {
			let expected = &expected_checksum;
			let actual = &actual_checksum;
			let error = tg::error!(
				code = tg::error::Code::ChecksumMismatch,
				%expected,
				%actual,
				"checksum mismatch",
			);
			(1, Some(error))
		};

		// Create an ID.
		let id = tg::process::Id::new();

		// Insert the process children.
		let statement = formatdoc!(
			"
				insert into process_children (process, position, child, options)
				select process, position, child, options from process_children where process = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Update parent depths.
		match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => {
				let statement = formatdoc!(
					"
						call update_parent_depths(array[{p}1]::text[]);
					"
				);
				let params = db::params![id.to_string()];
				transaction
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			},
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(transaction) => {
				Self::update_parent_depths_sqlite(transaction, vec![id.to_string()])
					.await
					.map_err(|source| tg::error!(!source, "failed to update parent depths"))?;
			},
		}

		let status = tg::process::Status::Finished;

		// Insert the process.
		let statement = formatdoc!(
			"
				insert into processes (
					id,
					actual_checksum,
					cacheable,
					command,
					created_at,
					error,
					error_code,
					exit,
					expected_checksum,
					finished_at,
					host,
					mounts,
					network,
					output,
					retry,
					status,
					token_count,
					touched_at
				)
				values (
					{p}1,
					{p}2,
					{p}3,
					{p}4,
					{p}5,
					{p}6,
					{p}7,
					{p}8,
					{p}9,
					{p}10,
					{p}11,
					{p}12,
					{p}13,
					{p}14,
					{p}15,
					{p}16,
					{p}17,
					{p}18
				)
				on conflict (id) do update set
					actual_checksum = {p}2,
					cacheable = {p}3,
					command = {p}4,
					created_at = {p}5,
					error = {p}6,
					error_code = {p}7,
					exit = {p}8,
					expected_checksum = {p}9,
					finished_at = {p}10,
					host = {p}11,
					mounts = {p}12,
					network = {p}13,
					output = {p}14,
					retry = {p}15,
					status = {p}16,
					token_count = {p}17,
					touched_at = {p}18;
			"
		);
		let now: i64 = time::OffsetDateTime::now_utc().unix_timestamp();
		let (error_data, error_code) = if let Some(error) = &error {
			error
				.store(self)
				.await
				.map_err(|source| tg::error!(!source, "failed to store the error"))?;
			let code = error.data(self).await?.code.map(|code| code.to_string());
			(Some(error.id()), code)
		} else {
			(None, None)
		};
		let params = db::params![
			id.to_string(),
			actual_checksum.to_string(),
			true,
			arg.command.item.to_string(),
			now,
			error_data.map(db::value::Json),
			error_code,
			exit,
			expected_checksum.to_string(),
			now,
			host,
			(!arg.mounts.is_empty()).then(|| db::value::Json(arg.mounts.clone())),
			arg.network,
			output.clone().map(db::value::Json),
			arg.retry,
			status.to_string(),
			0,
			now,
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(Some(LocalOutput {
			id,
			permit: None,
			status,
			token: None,
			wait: Some(tg::process::wait::Output {
				error: error.as_ref().map(tg::Error::to_data_or_id),
				exit,
				output,
			}),
		}))
	}

	async fn create_local_process(
		&self,
		transaction: &database::Transaction<'_>,
		arg: &tg::process::spawn::Arg,
		cacheable: bool,
		host: &str,
	) -> tg::Result<LocalOutput> {
		let p = transaction.p();

		// Create an ID.
		let id = tg::process::Id::new();

		// Create a token.
		let token = Self::create_process_token();

		// Attempt to acquire a permit immediately.
		let permit = 'a: {
			if let Some(parent_id) = &arg.parent
				&& let Some(parent_permit) = self.process_permits.get(parent_id)
			{
				let parent_permit = parent_permit.clone();
				if let Ok(guard) = parent_permit.try_lock_owned() {
					break 'a Some(ProcessPermit(tg::Either::Right(guard)));
				}
			}
			if let Ok(server_permit) = self.process_semaphore.clone().try_acquire_owned() {
				break 'a Some(ProcessPermit(tg::Either::Left(server_permit)));
			}
			None
		};

		// Set the status.
		let status = if permit.is_some() {
			tg::process::Status::Started
		} else {
			tg::process::Status::Enqueued
		};

		// Insert the process.
		let statement = formatdoc!(
			"
				insert into processes (
					id,
					cacheable,
					command,
					created_at,
					depth,
					enqueued_at,
					expected_checksum,
					heartbeat_at,
					host,
					mounts,
					network,
					retry,
					started_at,
					status,
					stderr,
					stdin,
					stdout,
					token_count,
					touched_at
				)
				values (
					{p}1,
					{p}2,
					{p}3,
					{p}4,
					{p}5,
					{p}6,
					{p}7,
					{p}8,
					{p}9,
					{p}10,
					{p}11,
					{p}12,
					{p}13,
					{p}14,
					{p}15,
					{p}16,
					{p}17,
					{p}18,
					{p}19
				)
				on conflict (id) do update set
					cacheable = {p}2,
					command = {p}3,
					created_at = {p}4,
					depth = {p}5,
					enqueued_at = {p}6,
					expected_checksum = {p}7,
					heartbeat_at = {p}8,
					host = {p}9,
					mounts = {p}10,
					network = {p}11,
					retry = {p}12,
					started_at = {p}13,
					status = {p}14,
					stderr = {p}15,
					stdin = {p}16,
					stdout = {p}17,
					token_count = {p}18,
					touched_at = {p}19;
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let heartbeat_at = if permit.is_some() { Some(now) } else { None };
		let started_at = if permit.is_some() { Some(now) } else { None };
		let params = db::params![
			id.to_string(),
			cacheable,
			arg.command.item.to_string(),
			now,
			1,
			now,
			arg.checksum.as_ref().map(ToString::to_string),
			heartbeat_at,
			host,
			(!arg.mounts.is_empty()).then(|| db::value::Json(arg.mounts.clone())),
			arg.network,
			arg.retry,
			started_at,
			status.to_string(),
			arg.stderr.as_ref().map(ToString::to_string),
			arg.stdin.as_ref().map(ToString::to_string),
			arg.stdout.as_ref().map(ToString::to_string),
			0,
			now,
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Insert the process token.
		let statement = formatdoc!(
			"
				insert into process_tokens (process, token)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![id.to_string(), token];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Update token count.
		let statement = formatdoc!(
			"
				update processes
				set token_count = token_count + 1
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(LocalOutput {
			id,
			permit,
			status,
			token: Some(token),
			wait: None,
		})
	}

	async fn try_get_cached_process_remote(
		&self,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		// Find a process.
		let streams = self
			.get_remote_clients()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remote clients"))?
			.into_iter()
			.map(|(name, client)| {
				let arg = arg.clone();
				async move {
					let arg = tg::process::spawn::Arg {
						cached: Some(true),
						local: None,
						parent: None,
						remotes: None,
						..arg.clone()
					};
					let stream = client
						.spawn_process(arg)
						.await?
						.map_ok(move |event| {
							event.map_output(|mut output| {
								output.remote.replace(name.clone());
								output
							})
						})
						.boxed();
					Ok::<_, tg::Error>(stream)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await
			.into_iter()
			.filter_map(std::result::Result::ok)
			.collect::<Vec<_>>(); // resolves https://github.com/rust-lang/rust/issues/64552

		// Wait for the first output.
		let stream = stream::iter(streams).flatten();
		let mut stream = pin!(stream);
		while let Some(event) = stream.next().await {
			if let Ok(tg::progress::Event::Output(output)) = event {
				return Ok(Some(output));
			}
		}
		Ok(None)
	}

	async fn add_process_child(
		&self,
		parent: &tg::process::Id,
		child: &tg::process::Id,
		options: &tg::referent::Options,
		token: Option<&String>,
	) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Add the process as a child.
		self.add_process_child_with_transaction(&transaction, parent, child, options, token)
			.await
			.map_err(
				|source| tg::error!(!source, %parent, %child, "failed to add the process as a child"),
			)?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Drop the connection.
		drop(connection);

		// Publish the child message.
		self.spawn_publish_process_child_message_task(parent);

		Ok(())
	}

	async fn add_process_child_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		parent: &tg::process::Id,
		child: &tg::process::Id,
		options: &tg::referent::Options,
		token: Option<&String>,
	) -> tg::Result<()> {
		let p = transaction.p();

		// Add the child to the database.
		let statement = formatdoc!(
			"
				insert into process_children (process, position, child, options, token)
				values ({p}1, (select coalesce(max(position) + 1, 0) from process_children where process = {p}1), {p}2, {p}3, {p}4)
				on conflict (process, child) do nothing;
			"
		);
		let params = db::params![
			parent.to_string(),
			child.to_string(),
			db::value::Json(options),
			token
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Update parent depths.
		match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => {
				let statement = formatdoc!(
					"
						call update_parent_depths(array[{p}1]::text[]);
					"
				);
				let params = db::params![child.to_string()];
				transaction
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			},
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(transaction) => {
				Self::update_parent_depths_sqlite(transaction, vec![child.to_string()])
					.await
					.map_err(|source| tg::error!(!source, "failed to update parent depths"))?;
			},
		}

		Ok(())
	}

	#[cfg(feature = "sqlite")]
	pub(crate) async fn update_parent_depths_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		child_ids: Vec<String>,
	) -> tg::Result<()> {
		let mut current_ids = child_ids;
		let mut visited = std::collections::HashSet::new();

		while !current_ids.is_empty() {
			let mut updated_ids = Vec::new();

			// Process each child to find and update its parents.
			for child_id in &current_ids {
				// Find parents of this child and their max child depth.
				#[derive(db::row::Deserialize)]
				struct Parent {
					process: String,
					max_child_depth: Option<i64>,
				}
				let statement = indoc!(
					"
						select process_children.process, max(processes.depth) as max_child_depth
						from process_children
						join processes on processes.id = process_children.child
						where process_children.child = ?1
						group by process_children.process;
					"
				);
				let params = db::params![child_id.clone()];
				let parents: Vec<Parent> = transaction
					.query_all_into::<Parent>(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to query parent depths"))?;

				// Update each parent's depth if needed.
				for parent in parents {
					// Skip parents that have already been visited to avoid infinite loops from cycles.
					if visited.contains(&parent.process) {
						continue;
					}
					if let Some(max_child_depth) = parent.max_child_depth {
						let statement = indoc!(
							"
								update processes
								set depth = max(depth, ?1)
								where id = ?2 and depth < ?1;
							"
						);
						let new_depth = max_child_depth + 1;
						let params = db::params![new_depth, parent.process.clone()];
						let rows = transaction
							.execute(statement.into(), params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to update parent depth")
							})?;

						// If we updated this parent, track it for next iteration.
						if rows > 0 {
							updated_ids.push(parent.process);
						}
					}
				}
			}

			// Mark the current batch as visited.
			visited.extend(current_ids);

			// Exit if no parents were updated.
			if updated_ids.is_empty() {
				break;
			}

			// Continue with the updated parents.
			current_ids = updated_ids;
		}

		Ok(())
	}

	fn spawn_publish_process_child_message_task(&self, parent: &tg::process::Id) {
		tokio::spawn({
			let server = self.clone();
			let id = parent.clone();
			async move {
				server
					.messenger
					.publish(format!("processes.{id}.children"), ())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish("watchdog".into(), ())
					.await
					.inspect_err(|error| {
						tracing::error!(?error, "failed to publish the watchdog message");
					})
					.ok();
			}
		});
	}

	fn spawn_process_parent_permit_task(
		&self,
		arg: &tg::process::spawn::Arg,
		id: &tg::process::Id,
	) {
		tokio::spawn({
			let server = self.clone();
			let parent = arg.parent.clone();
			let process = tg::Process::new(id.clone(), None, None, None, None);
			async move {
				// Acquire the parent's permit.
				let Some(permit) = parent.as_ref().and_then(|parent| {
					server
						.process_permits
						.get(parent)
						.map(|permit| permit.clone())
				}) else {
					return;
				};
				let permit = permit
					.lock_owned()
					.map(|guard| ProcessPermit(tg::Either::Right(guard)))
					.await;

				// Wait for any cleans to finish.
				let clean_guard = server.acquire_clean_guard().await;

				// Attempt to start the process.
				let arg = tg::process::start::Arg {
					local: None,
					remotes: process.remote().cloned().map(|remote| vec![remote]),
				};
				let result = server.start_process(process.id(), arg.clone()).await;
				if let Err(error) = result {
					tracing::trace!(error = %error.trace(), "failed to start the process");
					return;
				}

				// Spawn the process task.
				server.spawn_process_task(&process, permit, clean_guard);
			}
		});
	}

	fn create_process_token() -> String {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
	}

	pub(crate) async fn handle_spawn_process_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		let stream = self
			.try_spawn_process_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), BoxBody::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
