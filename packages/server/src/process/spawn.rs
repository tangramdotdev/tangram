use {
	crate::{Context, SandboxPermit, Server, database},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future,
		stream::{BoxStream, FuturesUnordered},
	},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{fmt::Write, pin::pin},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
	tangram_messenger::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

#[derive(derive_more::Debug)]
struct LocalOutput {
	cached: bool,
	id: tg::process::Id,
	#[debug(ignore)]
	permit: Option<SandboxPermit>,
	sandbox: tg::sandbox::Id,
	sandbox_status: Option<tg::sandbox::Status>,
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
		// If the process context is set, update the parent, location, and retry.
		if let Some(process) = &context.process {
			arg.debug = process.debug.clone();
			arg.parent = Some(process.id.clone());
			arg.location = process.location.clone().map(Into::into);
			arg.retry = process.retry;
		}
		let parent_sandbox = context.sandbox.clone();

		if arg.sandbox.is_none() {
			return Err(tg::error!(
				"unsandboxed processes cannot be spawned on the server"
			));
		}

		// Create the progress.
		let progress = crate::progress::Handle::new();

		// Spawn the task.
		let task = Task::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move |_| match Box::pin(server.try_spawn_process_task(
				arg,
				parent_sandbox,
				&progress,
			))
			.await
			{
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
		parent_sandbox: Option<tg::sandbox::Id>,
		progress: &crate::progress::Handle<Option<tg::process::spawn::Output>>,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let location = self.location(arg.location.as_ref())?;

		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.try_spawn_process_local(arg, parent_sandbox).await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => self.try_spawn_process_region(arg, progress, region).await?,
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.try_spawn_process_remote(arg, progress, remote, region)
					.await?
			},
		};

		Ok(output)
	}

	async fn try_spawn_process_local(
		&self,
		arg: tg::process::spawn::Arg,
		parent_sandbox: Option<tg::sandbox::Id>,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let tty = match arg.tty.as_ref() {
			None => None,
			Some(tty) => Some(
				tty.as_ref()
					.right()
					.copied()
					.ok_or_else(|| tg::error!("invalid tty"))?,
			),
		};

		// Get the host.
		let command_ = tg::Command::with_id(arg.command.item.clone());
		let host = command_.host_with_handle(self).await.ok();

		// Get a process store connection.
		let mut connection = self
			.process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Determine if the process is cacheable.
		let cacheable = if let Some(tg::Either::Left(sandbox)) = &arg.sandbox {
			sandbox.mounts.is_empty() && !sandbox.network
		} else {
			false
		};
		let cacheable = cacheable || arg.checksum.is_some();
		let cacheable = cacheable
			&& arg.stdin.is_null()
			&& arg.stdout.is_log()
			&& arg.stderr.is_log()
			&& tty.is_none();

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
				.create_local_process(
					&transaction,
					&arg,
					parent_sandbox.as_ref(),
					cacheable,
					&host,
				)
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

		// Wake the watchdog so depth-based limits are enforced promptly.
		if output
			.as_ref()
			.is_some_and(|output| !output.status.is_finished())
		{
			self.spawn_publish_watchdog_message_task();
		}

		// If the process is unfinished, then enqueue it on its sandbox process queue.
		if let Some(output) = &mut output
			&& !output.status.is_finished()
		{
			let sandbox = &output.sandbox;
			if let Some(permit) = output.permit.take() {
				self.spawn_sandbox_task(
					sandbox,
					tg::Location::Local(tg::location::Local::default()),
					permit,
					Some(output.id.clone()),
				);
			} else if output.sandbox_status == Some(tg::sandbox::Status::Created) {
				self.spawn_publish_sandbox_processes_created_message_task(sandbox);
				self.spawn_publish_sandboxes_created_message_task();
				if matches!(arg.sandbox, Some(tg::Either::Left(_))) {
					self.spawn_process_parent_permit_task(
						parent_sandbox.as_ref(),
						sandbox,
						&output.id,
					);
				}
			} else {
				self.spawn_publish_sandbox_processes_created_message_task(sandbox);
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

		// Create a future that will attempt to get a cached process in another region or on a remote if possible.
		let cached_future = async {
			if finished {
				return Ok::<_, tg::Error>(None);
			}
			if cacheable && matches!(arg.cached, None | Some(true)) {
				let locations =
					self.locations(arg.cache_location.as_ref())
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to resolve the cache locations")
						})?;
				let regions = locations.local.map_or_else(Vec::new, |local| local.regions);
				if let Some(output) = self
					.try_get_cached_process_regions(&arg, &regions)
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to get a cached process from another region"
						)
					})? {
					return Ok(Some(output));
				}
				let output = self
					.try_get_cached_process_remotes(&arg, &locations.remotes)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to get a cached process from a remote")
					})?;
				Ok(output)
			} else {
				Ok(None)
			}
		};

		// If the local process finishes before the cached lookup responds, then use the local process. If a cached process is found sooner, then spawn a task to cancel the local process and use the cached process.
		let output = match future::select(pin!(local_future), pin!(cached_future)).await {
			future::Either::Left((result, cached_future)) => {
				if let Some(wait) = result? {
					let output = output.unwrap();
					tg::process::spawn::Output {
						cached: output.cached,
						location: Some(tg::Location::Local(tg::location::Local::default())),
						process: tg::Either::Right(output.id),
						token: output.token,
						wait: Some(wait),
					}
				} else {
					let Some(cached_output) = cached_future.await? else {
						return Ok(None);
					};
					cached_output
				}
			},
			future::Either::Right((result, _)) => {
				if let Ok(Some(cached_output)) = result {
					if let Some(output) = output
						&& let Some(token) = output.token
					{
						tokio::spawn({
							let server = self.clone();
							async move {
								let arg = tg::process::cancel::Arg {
									location: Some(
										tg::Location::Local(tg::location::Local::default()).into(),
									),
									token,
								};
								server.cancel_process(&output.id, arg).boxed().await.ok();
							}
						});
					}
					cached_output
				} else {
					let Some(output) = output else {
						return Ok(None);
					};
					tg::process::spawn::Output {
						cached: output.cached,
						location: Some(tg::Location::Local(tg::location::Local::default())),
						process: tg::Either::Right(output.id),
						token: output.token,
						wait: output.wait,
					}
				}
			},
		};

		if let Some(parent) = &arg.parent {
			let child = output.process.as_ref().unwrap_right();
			self.add_process_child(
				parent,
				output.cached,
				child,
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

	async fn try_spawn_process_region(
		&self,
		arg: tg::process::spawn::Arg,
		progress: &crate::progress::Handle<Option<tg::process::spawn::Output>>,
		region: String,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let client = self.get_region_client(region.clone()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		self.spawn_process_push_command(arg.command.item(), Some(location.clone()), progress)
			.await
			.map_err(
				|source| tg::error!(!source, region = %region, "failed to push the command"),
			)?;
		let arg = tg::process::spawn::Arg {
			location: Some(location.clone().into()),
			..arg
		};
		let stream = client.try_spawn_process(arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to spawn the process"),
		)?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.next().await {
			let event = event.map(|event| {
				event.map_output(|output| {
					output.map(|mut output| {
						output.location = Some(location.clone());
						output
					})
				})
			});
			if let Some(output) = progress.forward(event) {
				return Ok(output);
			}
		}
		Err(tg::error!("expected an output"))
	}

	async fn try_spawn_process_remote(
		&self,
		arg: tg::process::spawn::Arg,
		progress: &crate::progress::Handle<Option<tg::process::spawn::Output>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let client = self.get_remote_client(remote.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote, "failed to get the remote client"),
		)?;
		let destination = tg::Location::Remote(tg::location::Remote {
			name: remote.clone(),
			region: region.clone(),
		});
		self.spawn_process_push_command(arg.command.item(), Some(destination), progress)
			.await
			.map_err(
				|source| tg::error!(!source, remote = %remote, "failed to push the command"),
			)?;
		let arg = tg::process::spawn::Arg {
			location: Some(
				tg::Location::Local(tg::location::Local {
					region: region.clone(),
				})
				.into(),
			),
			..arg
		};
		let stream = client.try_spawn_process(arg).await.map_err(
			|source| tg::error!(!source, remote = %remote, "failed to spawn the process"),
		)?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.next().await {
			let remote = remote.clone();
			let event = event.map(|event| {
				event.map_output(|output| {
					output.map(|mut output| {
						let region = match output.location.take() {
							Some(tg::Location::Local(local)) => local.region,
							Some(tg::Location::Remote(remote)) => remote.region,
							None => None,
						};
						output.location = Some(tg::Location::Remote(tg::location::Remote {
							name: remote.clone(),
							region,
						}));
						output
					})
				})
			});
			if let Some(output) = progress.forward(event) {
				return Ok(output);
			}
		}
		Err(tg::error!("expected an output"))
	}

	async fn spawn_process_push_command(
		&self,
		command: &tg::command::Id,
		location: Option<tg::location::Location>,
		progress: &crate::progress::Handle<Option<tg::process::spawn::Output>>,
	) -> tg::Result<()> {
		let push_arg = tg::push::Arg {
			commands: true,
			destination: location,
			items: vec![tg::Either::Left(command.clone().into())],
			..Default::default()
		};
		let stream = self
			.push(push_arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to push the command"))?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			if event.is_output() {
				return Ok(());
			}
			progress.forward(Ok(event));
		}
		Err(tg::error!("expected an output"))
	}

	async fn try_get_cached_process_regions(
		&self,
		arg: &tg::process::spawn::Arg,
		regions: &[String],
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_cached_process_region(arg, region))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_get_cached_process_region(
		&self,
		arg: &tg::process::spawn::Arg,
		region: &str,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::spawn::Arg {
			cached: Some(true),
			cache_location: Some(disabled_cache_locations()),
			location: Some(location.clone().into()),
			parent: None,
			..arg.clone()
		};
		let stream = client.try_spawn_process(arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the cached process"),
		)?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.next().await {
			let event = event?;
			let Some(mut output) = event.try_unwrap_output().ok().flatten() else {
				continue;
			};
			output.location = Some(location);
			return Ok(Some(output));
		}
		Ok(None)
	}

	async fn try_get_cached_process_remotes(
		&self,
		arg: &tg::process::spawn::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_cached_process_remote(arg, remote))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_get_cached_process_remote(
		&self,
		arg: &tg::process::spawn::Arg,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let client = self.get_remote_client(remote.name.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::process::spawn::Arg {
			cached: Some(true),
			cache_location: Some(disabled_cache_locations()),
			location: Some(tg::Location::Local(tg::location::Local { region: None }).into()),
			parent: None,
			..arg.clone()
		};
		let stream = client.try_spawn_process(arg).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, "failed to get the cached process"),
		)?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.next().await {
			let event = event?;
			let Some(mut output) = event.try_unwrap_output().ok().flatten() else {
				continue;
			};
			let region = match output.location.take() {
				Some(tg::Location::Local(local)) => local.region,
				Some(tg::Location::Remote(remote)) => remote.region,
				None => None,
			};
			output.location = Some(tg::Location::Remote(tg::location::Remote {
				name: remote.name.clone(),
				region,
			}));
			return Ok(Some(output));
		}
		Ok(None)
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
			sandbox: tg::sandbox::Id,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			sandbox_status: Option<tg::sandbox::Status>,
			#[tangram_database(as = "db::value::FromStr")]
			status: tg::process::Status,
			stored_at: i64,
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
				select
					processes.id,
					error,
					exit,
					output,
					processes.sandbox,
					sandboxes.status as sandbox_status,
					processes.status,
					processes.stored_at
				from processes
				left join sandboxes on sandboxes.id = processes.sandbox,
				params
				where
					processes.command = params.command and
					processes.cacheable = true and
					processes.expected_checksum {is} params.checksum and
					processes.error_code {isnt} 'cancellation' and
					processes.error_code {isnt} 'heartbeat_expiration' and
					processes.error_code {isnt} 'internal'
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
			sandbox,
			sandbox_status,
			status,
			stored_at,
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

		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let tti = self
			.config
			.process
			.time_to_index
			.as_secs()
			.to_i64()
			.unwrap();
		let max_stored_at = now - tti;
		if status.is_finished() && stored_at <= max_stored_at {
			let process = self
				.index
				.touch_process(&id, now, self.config.process.time_to_touch)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to touch the process"))?;
			if process.is_none() {
				return Ok(None);
			}
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
			if sandbox_status.is_some_and(|status| status.is_finished()) {
				return Ok(None);
			}

			let statement = formatdoc!(
				"
					update sandboxes
					set heartbeat_at = heartbeat_at
					where id = {p}1 and status != 'finished';
				"
			);
			let params = db::params![sandbox.to_string()];
			let n = transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			if n == 0 {
				return Ok(None);
			}

			let status = self
				.try_lock_process_with_transaction(transaction, &id)
				.await
				.map_err(|source| tg::error!(!source, "failed to lock the process"))?;
			let Some(status) = status else {
				return Ok(None);
			};
			if status.is_finished() {
				return Ok(None);
			}

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
			self.update_process_token_count_with_transaction(transaction, &id)
				.await
				.map_err(|source| tg::error!(!source, "failed to update the token count"))?;

			Some(token)
		};

		Ok(Some(LocalOutput {
			cached: true,
			id: id.clone(),
			permit: None,
			sandbox,
			sandbox_status,
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
			depth: Option<i64>,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::process::Id,
			#[tangram_database(as = "Option<db::value::Json<tg::value::Data>>")]
			output: Option<tg::value::Data>,
			#[tangram_database(as = "db::value::FromStr")]
			sandbox: tg::sandbox::Id,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			sandbox_status: Option<tg::sandbox::Status>,
			#[tangram_database(as = "db::value::FromStr")]
			status: tg::process::Status,
			stored_at: i64,
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
				select
					actual_checksum,
					processes.depth,
					processes.id,
					output,
					processes.sandbox,
					sandboxes.status as sandbox_status,
					processes.status,
					processes.stored_at
				from processes
				left join sandboxes on sandboxes.id = processes.sandbox,
				params
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
			depth,
			id: source,
			output,
			sandbox,
			sandbox_status,
			status: source_status,
			stored_at: source_stored_at,
		}) = transaction
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		let now: i64 = time::OffsetDateTime::now_utc().unix_timestamp();
		let tti = self
			.config
			.process
			.time_to_index
			.as_secs()
			.to_i64()
			.unwrap();
		let max_stored_at = now - tti;
		if source_status.is_finished() && source_stored_at <= max_stored_at {
			let process = self
				.index
				.touch_process(&source, now, self.config.process.time_to_touch)
				.await
				.map_err(|error| tg::error!(!error, id = %source, "failed to touch the process"))?;
			if process.is_none() {
				return Ok(None);
			}
		}

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

		let status = tg::process::Status::Finished;
		let stderr_open = match &arg.stderr {
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Some(!status.is_finished()),
			tg::process::Stdio::Blob(_)
			| tg::process::Stdio::Inherit
			| tg::process::Stdio::Log
			| tg::process::Stdio::Null => None,
		};
		let stdin_open = match &arg.stdin {
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Some(!status.is_finished()),
			tg::process::Stdio::Blob(_)
			| tg::process::Stdio::Inherit
			| tg::process::Stdio::Log
			| tg::process::Stdio::Null => None,
		};
		let stdout_open = match &arg.stdout {
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Some(!status.is_finished()),
			tg::process::Stdio::Blob(_)
			| tg::process::Stdio::Inherit
			| tg::process::Stdio::Log
			| tg::process::Stdio::Null => None,
		};

		// Insert the process.
		let statement = formatdoc!(
			"
				insert into processes (
					actual_checksum,
					cacheable,
					command,
					created_at,
					debug,
					depth,
					error,
					error_code,
					exit,
					expected_checksum,
					finished_at,
					host,
					id,
					output,
					retry,
					sandbox,
					status,
					stderr_open,
					stdin_open,
					stdout_open,
					stored_at,
					token_count,
					tty
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
					{p}19,
					{p}20,
					{p}21,
					{p}22,
					{p}23
				);
			"
		);
		let (error_data, error_code) = if let Some(error) = &error {
			error
				.store_with_handle(self)
				.await
				.map_err(|source| tg::error!(!source, "failed to store the error"))?;
			let code = error
				.data_with_handle(self)
				.await?
				.code
				.map(|code| code.to_string());
			(Some(error.id()), code)
		} else {
			(None, None)
		};
		let tty = match arg.tty.as_ref() {
			None => None,
			Some(tty) => Some(
				tty.as_ref()
					.right()
					.copied()
					.ok_or_else(|| tg::error!("invalid tty"))?,
			),
		};
		let params = db::params![
			actual_checksum.to_string(),
			true,
			arg.command.item.to_string(),
			now,
			arg.debug.clone().map(db::value::Json),
			depth,
			error_data.map(|id| id.to_string()),
			error_code,
			exit,
			expected_checksum.to_string(),
			now,
			host,
			id.to_string(),
			output.clone().map(db::value::Json),
			arg.retry,
			sandbox.to_string(),
			status.to_string(),
			stderr_open,
			stdin_open,
			stdout_open,
			now,
			0,
			tty.map(db::value::Json),
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Copy the process children.
		let statement = formatdoc!(
			"
				insert into process_children (
					process,
					position,
					cached,
					child,
					options,
					token
				)
				select
					{p}1,
					position,
					cached,
					child,
					options,
					token
				from process_children
				where process = {p}2
				on conflict (process, child) do nothing;
			"
		);
		let params = db::params![id.to_string(), source.to_string()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(Some(LocalOutput {
			cached: true,
			id,
			permit: None,
			sandbox,
			sandbox_status,
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
		parent_sandbox: Option<&tg::sandbox::Id>,
		cacheable: bool,
		host: &str,
	) -> tg::Result<LocalOutput> {
		let p = transaction.p();

		// Create an ID.
		let id = tg::process::Id::new();

		// Create a token.
		let token = Self::create_process_token();

		let parent_sandbox = parent_sandbox.cloned();

		let (sandbox, sandbox_status, permit) = match &arg.sandbox {
			None => return Err(tg::error!("expected the sandbox to be set")),
			Some(tg::Either::Left(arg)) => {
				let permit = self.try_acquire_sandbox_permit(parent_sandbox.as_ref());
				let status = if permit.is_some() {
					tg::sandbox::Status::Started
				} else {
					tg::sandbox::Status::Created
				};
				let sandbox = self
					.create_local_sandbox_with_transaction(transaction, arg, status)
					.await?;
				(sandbox, status, permit)
			},
			Some(tg::Either::Right(sandbox)) => {
				let status = self
					.try_get_sandbox_status_with_transaction(transaction, sandbox)
					.await?
					.ok_or_else(|| tg::error!("failed to find the sandbox"))?;
				if status.is_finished() {
					return Err(tg::error!("the sandbox is finished"));
				}
				(sandbox.clone(), status, None)
			},
		};

		let status = if permit.is_some() {
			tg::process::Status::Started
		} else {
			tg::process::Status::Created
		};
		let stderr_open = match &arg.stderr {
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Some(!status.is_finished()),
			tg::process::Stdio::Blob(_)
			| tg::process::Stdio::Inherit
			| tg::process::Stdio::Log
			| tg::process::Stdio::Null => None,
		};
		let stdin_open = match &arg.stdin {
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Some(!status.is_finished()),
			tg::process::Stdio::Blob(_)
			| tg::process::Stdio::Inherit
			| tg::process::Stdio::Log
			| tg::process::Stdio::Null => None,
		};
		let stdout_open = match &arg.stdout {
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Some(!status.is_finished()),
			tg::process::Stdio::Blob(_)
			| tg::process::Stdio::Inherit
			| tg::process::Stdio::Log
			| tg::process::Stdio::Null => None,
		};

		// Insert the process.
		let statement = formatdoc!(
			"
				insert into processes (
					cacheable,
					command,
					created_at,
					debug,
					depth,
					expected_checksum,
					host,
					id,
					retry,
					sandbox,
					started_at,
					status,
					stderr,
					stderr_open,
					stdin,
					stdin_open,
					stdout,
					stdout_open,
					stored_at,
					token_count,
					tty
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
					{p}19,
					{p}20,
					{p}21
				);
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let started_at = (status == tg::process::Status::Started).then_some(now);
		let tty = match arg.tty.as_ref() {
			None => None,
			Some(tty) => Some(
				tty.as_ref()
					.right()
					.copied()
					.ok_or_else(|| tg::error!("invalid tty"))?,
			),
		};
		let params = db::params![
			cacheable,
			arg.command.item.to_string(),
			now,
			arg.debug.clone().map(db::value::Json),
			1,
			arg.checksum.as_ref().map(ToString::to_string),
			host,
			id.to_string(),
			arg.retry,
			sandbox.to_string(),
			started_at,
			status.to_string(),
			(!arg.stderr.is_null()).then(|| arg.stderr.to_string()),
			stderr_open,
			(!arg.stdin.is_null()).then(|| arg.stdin.to_string()),
			stdin_open,
			(!arg.stdout.is_null()).then(|| arg.stdout.to_string()),
			stdout_open,
			now,
			0,
			tty.map(db::value::Json),
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
		self.update_process_token_count_with_transaction(transaction, &id)
			.await
			.map_err(|source| tg::error!(!source, "failed to update the token count"))?;

		Ok(LocalOutput {
			cached: false,
			id,
			permit,
			sandbox,
			sandbox_status: Some(sandbox_status),
			status,
			token: Some(token),
			wait: None,
		})
	}

	async fn create_local_sandbox_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		arg: &tg::sandbox::create::Arg,
		status: tg::sandbox::Status,
	) -> tg::Result<tg::sandbox::Id> {
		let p = transaction.p();
		let id = tg::sandbox::Id::new();
		let statement = formatdoc!(
			"
				insert into sandboxes (
					id,
					cpu,
					created_at,
					heartbeat_at,
					hostname,
					memory,
					mounts,
					network,
					started_at,
					status,
					ttl,
					\"user\"
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
					{p}12
				);
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let heartbeat_at = (status == tg::sandbox::Status::Started).then_some(now);
		let started_at = (status == tg::sandbox::Status::Started).then_some(now);
		let isolation = self.resolve_sandbox_isolation()?;
		Self::validate_sandbox_resources(isolation, arg.cpu, arg.memory)?;
		let cpu = arg
			.cpu
			.map(i64::try_from)
			.transpose()
			.map_err(|source| tg::error!(!source, "invalid sandbox cpu"))?;
		let memory = arg
			.memory
			.map(i64::try_from)
			.transpose()
			.map_err(|source| tg::error!(!source, "invalid sandbox memory"))?;
		let ttl =
			i64::try_from(arg.ttl).map_err(|source| tg::error!(!source, "invalid sandbox ttl"))?;
		let params = db::params![
			id.to_string(),
			cpu,
			now,
			heartbeat_at,
			arg.hostname.clone(),
			memory,
			(!arg.mounts.is_empty()).then(|| db::value::Json(arg.mounts.clone())),
			arg.network,
			started_at,
			status.to_string(),
			ttl,
			arg.user.clone(),
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(id)
	}

	async fn try_get_sandbox_status_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::Status>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select status
				from sandboxes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let status = transaction
			.query_optional_value_into::<db::value::Serde<tg::sandbox::Status>>(
				statement.into(),
				params,
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|value| value.0);
		Ok(status)
	}

	fn try_acquire_sandbox_permit(
		&self,
		parent_sandbox: Option<&tg::sandbox::Id>,
	) -> Option<SandboxPermit> {
		if let Some(parent_sandbox) = parent_sandbox
			&& let Some(parent_permit) = self.sandbox_permits.get(parent_sandbox)
		{
			let parent_permit = parent_permit.clone();
			if let Ok(guard) = parent_permit.try_lock_owned() {
				return Some(SandboxPermit(tg::Either::Right(guard)));
			}
		}
		self.sandbox_semaphore
			.clone()
			.try_acquire_owned()
			.ok()
			.map(|permit| SandboxPermit(tg::Either::Left(permit)))
	}

	async fn add_process_child(
		&self,
		parent: &tg::process::Id,
		cached: bool,
		child: &tg::process::Id,
		options: &tg::referent::Options,
		token: Option<&String>,
	) -> tg::Result<()> {
		// Get a process store connection.
		let mut connection = self
			.process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Add the process as a child.
		self.add_process_child_with_transaction(
			&transaction,
			parent,
			cached,
			child,
			options,
			token,
		)
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

		// Wake the watchdog so parent depth changes are observed promptly.
		self.spawn_publish_watchdog_message_task();

		Ok(())
	}

	async fn add_process_child_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		parent: &tg::process::Id,
		cached: bool,
		child: &tg::process::Id,
		options: &tg::referent::Options,
		token: Option<&String>,
	) -> tg::Result<()> {
		let p = transaction.p();

		// Lock the parent and ensure that it is not finished.
		let status = self
			.try_lock_process_with_transaction(transaction, parent)
			.await
			.map_err(|source| tg::error!(!source, "failed to lock the parent process"))?;
		let Some(status) = status else {
			return Err(tg::error!("the parent process was not found"));
		};
		if status.is_finished() {
			return Err(tg::error!("the parent process was finished"));
		}

		// Determine if adding this child process creates a cycle.
		let statement = formatdoc!(
			"
				with recursive ancestors as (
					select {p}1 as id
					union all
					select process_children.process as id
					from ancestors
					join process_children on ancestors.id = process_children.child
				)
				select exists(
					select 1 from ancestors where id = {p}2
				);
			"
		);
		let params = db::params![parent.to_string(), child.to_string()];
		let cycle = transaction
			.query_one_value_into::<bool>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the cycle check"))?;

		// If adding this child creates a cycle, return an error.
		if cycle {
			// Try to reconstruct the cycle path by walking from the child through its descendants until we find a path back to the parent.
			let statement = formatdoc!(
				"
					with recursive reachable (current_process, path) as (
						select {p}2, {p}2

						union

						select pc.child, r.path || ' ' || pc.child
						from reachable r
						join process_children pc on r.current_process = pc.process
						where r.path not like '%' || pc.child || '%'
					)
					select
						{p}1 || ' ' || path as cycle
					from reachable
					where current_process = {p}1
					limit 1;
				"
			);
			let params = db::params![parent.to_string(), child.to_string()];
			let cycle = transaction
				.query_one_value_into::<String>(statement.into(), params)
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to get the cycle"))
				.ok();
			let mut message = String::from("adding this child process creates a cycle");
			if let Some(cycle) = cycle {
				let processes = cycle.split(' ').collect::<Vec<_>>();
				for i in 0..processes.len() - 1 {
					let parent = processes[i];
					let child = processes[i + 1];
					if i == 0 {
						write!(&mut message, "\n{parent} tried to add child {child}").unwrap();
					} else {
						write!(&mut message, "\n{parent} has child {child}").unwrap();
					}
				}
			}
			return Err(tg::error!("{message}"));
		}

		// Add the child to the process store.
		let statement = formatdoc!(
			"
				insert into process_children (
					process,
					position,
					cached,
					child,
					options,
					token
				) values (
					{p}1,
					(select coalesce(max(position) + 1, 0) from process_children where process = {p}1),
					{p}2,
					{p}3,
					{p}4,
					{p}5
				)
				on conflict (process, child) do nothing;
			"
		);
		let params = db::params![
			parent.to_string(),
			cached,
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
			database::Transaction::Postgres(transaction) => {
				Self::update_parent_depths_postgres(transaction, child.to_string())
					.await
					.map_err(|source| tg::error!(!source, "failed to update parent depths"))?;
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
	}

	fn spawn_process_parent_permit_task(
		&self,
		parent_sandbox: Option<&tg::sandbox::Id>,
		id: &tg::sandbox::Id,
		process: &tg::process::Id,
	) {
		tokio::spawn({
			let server = self.clone();
			let parent_sandbox = parent_sandbox.cloned();
			let sandbox = id.clone();
			let process = process.clone();
			async move {
				let Some(parent_sandbox) = parent_sandbox.as_ref() else {
					return;
				};

				let Some(permit) = server
					.sandbox_permits
					.get(parent_sandbox)
					.map(|permit| permit.clone())
				else {
					return;
				};
				let permit = permit
					.lock_owned()
					.map(|guard| SandboxPermit(tg::Either::Right(guard)))
					.await;

				let Ok(started) = server.try_start_sandbox_local(&sandbox).await.inspect_err(
					|error| tracing::trace!(error = %error.trace(), "failed to start the sandbox"),
				) else {
					return;
				};
				if !started {
					return;
				}

				let Ok(started) = server.try_start_process_local(&process).await.inspect_err(
					|error| tracing::trace!(error = %error.trace(), "failed to start the process"),
				) else {
					return;
				};
				if !started {
					return;
				}

				server.spawn_sandbox_task(
					&sandbox,
					tg::Location::Local(tg::location::Local::default()),
					permit,
					Some(process),
				);
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

fn disabled_cache_locations() -> tg::location::Arg {
	tg::location::Arg::default()
}
