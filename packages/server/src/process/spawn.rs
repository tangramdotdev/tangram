use {
	crate::{Server, Session, database, run::SpawnSandboxTaskArg},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future,
		stream::{BoxStream, FuturesUnordered},
	},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{fmt::Write, ops::ControlFlow, pin::pin},
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
#[cfg(feature = "turso")]
mod turso;

#[derive(derive_more::Debug)]
struct LocalOutput {
	cached: bool,
	id: tg::process::Id,
	lease: Option<String>,
	owner: Option<tg::Principal>,
	process_token: Option<String>,
	sandbox: tg::sandbox::create::Output,
	status: tg::process::Status,
	#[debug(ignore)]
	token: Option<tg::grant::Token>,
	wait: Option<tg::process::wait::Output>,
}

struct AddProcessChildArg<'a> {
	cached: bool,
	child: &'a tg::process::Id,
	command: &'a tg::command::Id,
	lease: Option<&'a String>,
	options: &'a tg::referent::Options,
	parent: &'a tg::process::Id,
	sandbox: Option<&'a tg::sandbox::Id>,
}

impl Session {
	pub async fn try_spawn_process(
		&self,
		mut arg: tg::process::spawn::Arg,
	) -> tg::Result<
		BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>,
	> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}

		// If the authentication is from a process, then update the parent, location, and retry.
		let authenticated_process = match &self.context.principal {
			tg::Principal::Process(process) => self.try_get_authenticated_process(process).await?,
			_ => None,
		};
		if let Some(process) = &authenticated_process
			&& let tg::Principal::Process(id) = &self.context.principal
		{
			arg.debug = process.debug.clone();
			arg.parent = Some(id.clone());
			arg.location = process.location.clone().map(Into::into);
			arg.retry = process.retry;
		}

		// Get the parent sandbox if there is one.
		let parent_sandbox = authenticated_process
			.as_ref()
			.map(|process| process.sandbox.clone());

		if arg.sandbox.is_none() {
			return Err(tg::error!(
				"unsandboxed processes cannot be spawned on the server"
			));
		}

		// Create the progress.
		let progress = crate::progress::Handle::new();

		// Spawn the task.
		let task = Task::spawn({
			let session = self.clone();
			let progress = progress.clone();
			async move |_| match Box::pin(session.try_spawn_process_task(
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
		let location = self.server.location(arg.location.as_ref())?;

		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.try_spawn_process_local(arg, parent_sandbox)
					.boxed()
					.await?
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
		// Authorize the command.
		let permission =
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree);
		let command = tg::object::Id::from(arg.command.item.clone());
		let command = if let Some(token) = arg.command.options.token.clone() {
			tg::Either::Right(tg::WithToken { id: command, token })
		} else {
			tg::Either::Left(command)
		};
		if !self
			.authorize(command, permission)
			.await?
			.is_some_and(|permissions| permissions.contains(permission))
		{
			return Err(tg::error!("unauthorized"));
		}

		// Get the host.
		let command_ = tg::Command::with_id(arg.command.item.clone());
		let host = command_
			.host_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the command host"))?
			.to_string();

		// Determine if the process is cacheable.
		let cacheable = if let Some(tg::Either::Left(sandbox)) = &arg.sandbox {
			sandbox.mounts.is_empty() && sandbox.network.is_none()
		} else {
			false
		};
		let cacheable = cacheable || arg.checksum.is_some();
		let cacheable = cacheable
			&& arg.stdin.is_null()
			&& arg.stdout.is_log()
			&& arg.stderr.is_log()
			&& arg.tty.is_none();

		// Authorize assigning the sandbox owner.
		if let Some(tg::Either::Left(sandbox)) = &arg.sandbox {
			self.authorize_owner(sandbox.owner.as_ref()).await?;
		}

		// Try to acquire a permit to run the process on this runner. If a permit is acquired, then
		// the process is created as claimed and run directly. Otherwise, it is scheduled.
		let mut permit = match &arg.sandbox {
			Some(tg::Either::Left(_)) => self.try_acquire_sandbox_permit(parent_sandbox.as_ref()),
			_ => None,
		};

		// Get or create a local process in the process store.
		let has_permit = permit.is_some();
		let session = self.clone();
		let mut output = self
			.server
			.process_store
			.run(|transaction| {
				let arg = arg.clone();
				let host = host.clone();
				let parent_sandbox = parent_sandbox.clone();
				let session = session.clone();
				async move {
					session
						.try_spawn_process_local_with_transaction(
							transaction,
							&arg,
							parent_sandbox.as_ref(),
							cacheable,
							has_permit,
							Some(host.as_str()),
						)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to spawn the process"))?;

		// Index the process if necessary.
		let local_output = output
			.as_ref()
			.map(|output| (output.id.clone(), output.sandbox.id.clone()));
		let index_batch_arg = if let Some(output) = &output
			&& !output.cached
		{
			let command = arg.command.item.clone();
			let id = output.id.clone();
			let now = time::OffsetDateTime::now_utc().unix_timestamp();
			let put_process_arg = tangram_index::process::put::Arg {
				children: None,
				command: command.into(),
				error: None,
				id: id.clone(),
				log: None,
				metadata: tg::process::Metadata::default(),
				output: None,
				parent: None,
				sandbox: Some(output.sandbox.id.clone()),
				stored: tangram_index::process::Stored::default(),
				touched_at: now,
			};
			let put_sandbox_arg = tangram_index::sandbox::put::Arg {
				id: output.sandbox.id.clone(),
				owner: output.owner.clone(),
			};
			Some(tangram_index::batch::Arg {
				put_processes: vec![put_process_arg],
				put_sandboxes: vec![put_sandbox_arg],
				..Default::default()
			})
		} else {
			None
		};
		if let Some(index_batch_arg) = index_batch_arg {
			self.server
				.index_tasks
				.spawn(|_| {
					let server = self.server.clone();
					async move {
						let result = server.index.batch(index_batch_arg).await;
						if let Err(error) = result {
							tracing::error!(error = %error.trace(), "failed to put process to index");
						}
					}
				})
				.detach();
		}

		// Wake the watchdog so depth-based limits are enforced promptly.
		if output
			.as_ref()
			.is_some_and(|output| !output.status.is_finished())
		{
			self.server.spawn_publish_watchdog_message_task();
		}

		// If the process is unfinished, then dispatch it.
		if let Some(output) = &mut output
			&& !output.status.is_finished()
			&& !output.cached
		{
			let sandbox = output.sandbox.id.clone();
			let sandbox_token = output.sandbox.token.clone();
			let process_token = output.process_token.clone();
			if let Some(permit) = permit.take() {
				self.server.spawn_sandbox_task(SpawnSandboxTaskArg {
					id: sandbox,
					location: tg::Location::Local(tg::location::Local::default()),
					permit,
					process: Some(output.id.clone()),
					process_token,
					started: None,
					token: sandbox_token,
				});
			} else if let Some(tg::Either::Left(sandbox_arg)) = &arg.sandbox {
				if let Some(parent_sandbox) = parent_sandbox.as_ref()
					&& self.server.sandbox_permits.contains_key(parent_sandbox)
				{
					self.spawn_process_parent_permit_task(
						parent_sandbox,
						&sandbox,
						&output.id,
						process_token.clone(),
						sandbox_token.clone(),
					);
				}

				// A new sandbox runs the process, so it must run on a runner matching the process host.
				let mut sandbox_arg = sandbox_arg.clone();
				if sandbox_arg.host.is_none() {
					sandbox_arg.host = Some(host.clone());
				}
				self.spawn_process_scheduler_task(
					&sandbox,
					&output.id,
					Some(sandbox_arg),
					sandbox_token,
					process_token,
				);
			} else {
				self.spawn_process_scheduler_task(
					&sandbox,
					&output.id,
					None,
					sandbox_token,
					process_token,
				);
			}
		}

		// Determine if the local process is finished.
		let finished = output
			.as_ref()
			.is_some_and(|output| output.status.is_finished());

		// Create a future that will await the local process if there is one.
		let local_future = {
			let id = output.as_ref().map(|output| output.id.clone());
			let token = output.as_ref().and_then(|output| output.token.clone());
			let wait = output.as_ref().and_then(|output| output.wait.clone());
			async move {
				if finished {
					return Ok::<_, tg::Error>(wait);
				}
				if let Some(id) = id {
					let arg = tg::process::wait::Arg {
						token,
						..Default::default()
					};
					let wait = self.wait_process(&id, arg).await.map_err(
						|error| tg::error!(!error, %id, "failed to wait for the process"),
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
				let locations = self
					.locations(arg.cache_location.as_ref())
					.await
					.map_err(|error| tg::error!(!error, "failed to resolve the cache locations"))?;
				let regions = locations.local.map_or_else(Vec::new, |local| local.regions);
				if let Some(output) = self
					.try_get_cached_process_regions(&arg, &regions)
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to get a cached process from another region")
					})? {
					return Ok(Some(output));
				}
				let output = self
					.try_get_cached_process_remotes(&arg, &locations.remotes)
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to get a cached process from a remote")
					})?;
				Ok(output)
			} else {
				Ok(None)
			}
		};

		// If the local process finishes before the cached lookup responds, then use the local process. If a cached process is found sooner, then spawn a task to cancel the local process and use the cached process.
		let mut output = match future::select(pin!(local_future), pin!(cached_future)).await {
			future::Either::Left((result, cached_future)) => {
				if let Some(wait) = result? {
					let output = output.unwrap();
					tg::process::spawn::Output {
						cached: output.cached,
						lease: output.lease,
						location: Some(tg::Location::Local(tg::location::Local::default())),
						process: tg::Either::Right(output.id),
						token: output.token,
						sandbox: Some(output.sandbox),
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
						&& let Some(lease) = output.lease
					{
						tokio::spawn({
							let session = self.clone();
							async move {
								let arg = tg::process::cancel::Arg {
									location: Some(
										tg::Location::Local(tg::location::Local::default()).into(),
									),
									lease,
								};
								session.cancel_process(&output.id, arg).boxed().await.ok();
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
						lease: output.lease,
						location: Some(tg::Location::Local(tg::location::Local::default())),
						process: tg::Either::Right(output.id),
						token: output.token,
						sandbox: Some(output.sandbox),
						wait: output.wait,
					}
				}
			},
		};
		if matches!(
			output.location,
			Some(tg::Location::Local(tg::location::Local { region: None }))
		) && let Some(wait) = &mut output.wait
			&& let Some(output) = &mut wait.output
		{
			self.add_tokens_to_value_data(output)?;
		}

		if let Some(parent) = &arg.parent {
			let child = output.process.as_ref().unwrap_right();
			let sandbox = local_output
				.as_ref()
				.and_then(|(id, sandbox)| (id == child).then(|| sandbox.clone()));
			self.add_process_child(AddProcessChildArg {
				cached: output.cached,
				child,
				command: &arg.command.item,
				lease: output.lease.as_ref(),
				options: &arg.command.options,
				parent,
				sandbox: sandbox.as_ref(),
			})
			.await
			.map_err(
				|error| tg::error!(!error, %parent, child = %output.process, "failed to add the process as a child"),
			)?;
		}

		// Make the process public if requested.
		if arg.public
			&& let tg::Either::Right(id) = &output.process
		{
			self.spawn_create_public_grant(id).await?;
		}

		Ok(Some(output))
	}

	async fn spawn_create_public_grant(&self, id: &tg::process::Id) -> tg::Result<()> {
		let resource = tg::Id::from(id.clone());
		let existing =
			{
				let mut connection =
					self.server.database.connection().await.map_err(|error| {
						tg::error!(!error, "failed to get a database connection")
					})?;
				let transaction = connection
					.transaction()
					.await
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				Self::list_resource_grants_with_transaction(&transaction, &resource).await?
			};
		let mut covered = tg::grant::permission::process::Set::empty();
		for grant in existing {
			if grant.principal == tg::grant::Principal::Public
				&& let tg::grant::permission::Set::Process(set) = grant.permissions
			{
				covered.insert(set);
			}
		}

		let mut missing = tg::grant::permission::process::Set::empty();
		for permission in [
			tg::grant::permission::process::Permission::Subtree,
			tg::grant::permission::process::Permission::SubtreeCommand,
			tg::grant::permission::process::Permission::SubtreeError,
			tg::grant::permission::process::Permission::SubtreeLog,
			tg::grant::permission::process::Permission::SubtreeOutput,
		] {
			let set = tg::grant::permission::process::Set::from_permission(permission);
			if !covered.contains(set) {
				missing.insert(set);
			}
		}
		if !missing.is_empty() {
			self.create_grant(tg::grant::create::Arg {
				principal: tg::principal::Selector::Principal(tg::grant::Principal::Public),
				permissions: tg::Either::Left(tg::grant::permission::Set::Process(missing)),
				resource: tg::grant::Resource::Id(resource),
			})
			.await?;
		}
		Ok(())
	}

	fn create_process_wait_token(
		&self,
		id: &tg::process::Id,
		now: i64,
	) -> tg::Result<Option<tg::grant::Token>> {
		let expires_at = now
			+ self
				.server
				.config
				.process
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		self.create_token(
			tg::grant::Resource::Id(id.clone().into()),
			vec![
				tg::grant::Permission::Process(tg::grant::permission::process::Permission::Node),
				tg::grant::Permission::Process(
					tg::grant::permission::process::Permission::NodeOutput,
				),
			],
			expires_at,
		)
	}

	async fn try_spawn_process_region(
		&self,
		arg: tg::process::spawn::Arg,
		progress: &crate::progress::Handle<Option<tg::process::spawn::Output>>,
		region: String,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		self.spawn_process_push_command(arg.command.item(), Some(location.clone()), progress)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to push the command"))?;
		let arg = tg::process::spawn::Arg {
			location: Some(location.clone().into()),
			..arg
		};
		let stream = client
			.try_spawn_process(arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to spawn the process"))?;
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
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
		)?;
		let destination = tg::Location::Remote(tg::location::Remote {
			name: remote.clone(),
			region: region.clone(),
		});
		self.spawn_process_push_command(arg.command.item(), Some(destination), progress)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to push the command"))?;
		let arg = tg::process::spawn::Arg {
			location: Some(
				tg::Location::Local(tg::location::Local {
					region: region.clone(),
				})
				.into(),
			),
			..arg
		};
		let stream = client
			.try_spawn_process(arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to spawn the process"))?;
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
		location: Option<tg::Location>,
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
			.map_err(|error| tg::error!(!error, "failed to push the command"))?;
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
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
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
			|error| tg::error!(!error, region = %region, "failed to get the cached process"),
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
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::process::spawn::Arg {
			cached: Some(true),
			cache_location: Some(disabled_cache_locations()),
			location: Some(tg::Location::Local(tg::location::Local { region: None }).into()),
			parent: None,
			..arg.clone()
		};
		let stream = client.try_spawn_process(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the cached process"),
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

	async fn try_spawn_process_local_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		arg: &tg::process::spawn::Arg,
		parent_sandbox: Option<&tg::sandbox::Id>,
		cacheable: bool,
		has_permit: bool,
		host: Option<&str>,
	) -> tg::Result<ControlFlow<Option<LocalOutput>, database::Error>> {
		let mut output = None;
		let owner = self
			.resolve_spawn_sandbox_owner_with_transaction(transaction, arg, parent_sandbox)
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the sandbox owner"))?;
		if cacheable && matches!(arg.cached, None | Some(true)) {
			let result = self
				.try_get_cached_process_local(transaction, arg, owner.as_ref())
				.await
				.map_err(|error| tg::error!(!error, "failed to get a cached local process"))?;
			output = match result {
				ControlFlow::Break(output) => output,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			if let Some(ref output) = output {
				tracing::trace!(?output, "got cached local process");
			} else if let Some(host) = host {
				let result = self
					.try_get_cached_process_with_mismatched_checksum_local(
						transaction,
						arg,
						host,
						owner.as_ref(),
					)
					.await
					.map_err(|error| {
						tg::error!(
							!error,
							"failed to get a cached local process with mismatched checksum"
						)
					})?;
				let cached_output = match result {
					ControlFlow::Break(output) => output,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				if let Some(ref output) = cached_output {
					tracing::trace!(?output, "got cached local process with mismatched checksum");
				}
				output = cached_output;
			}
		}
		let output = if let Some(output) = output {
			Some(output)
		} else if matches!(arg.cached, None | Some(false)) {
			let host = host.ok_or_else(|| tg::error!("expected the host to be set"))?;
			let output = match self
				.create_local_process(
					transaction,
					arg,
					parent_sandbox,
					cacheable,
					has_permit,
					host,
					owner,
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to create a local process"))?
			{
				ControlFlow::Break(output) => output,
				ControlFlow::Continue(error) => {
					return Ok(ControlFlow::Continue(error));
				},
			};
			tracing::trace!(?output, "created local process");
			Some(output)
		} else {
			None
		};

		Ok(ControlFlow::Break(output))
	}

	async fn try_get_cached_process_local(
		&self,
		transaction: &database::Transaction<'_>,
		arg: &tg::process::spawn::Arg,
		owner: Option<&tg::Principal>,
	) -> tg::Result<ControlFlow<Option<LocalOutput>, database::Error>> {
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
			#[cfg(feature = "turso")]
			database::Transaction::Turso(_) => "with params as (select ?1 as command, ?2 as checksum)",
		};
		let is = match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => "is not distinct from",
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(_) => "is",
			#[cfg(feature = "turso")]
			database::Transaction::Turso(_) => "is",
		};
		let isnt = match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => "is distinct from",
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(_) => "is not",
			#[cfg(feature = "turso")]
			database::Transaction::Turso(_) => "is not",
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
				order by processes.created_at desc;
			"
		);
		let params = db::params![
			arg.command.item.to_string(),
			arg.checksum.as_ref().map(ToString::to_string),
		];
		let rows = {
			let result = transaction
				.query_all_into::<Row>(statement.into(), params)
				.await;
			crate::database::retry!(result, "failed to execute the statement")
		};

		// Authorize.
		let permission =
			tg::grant::Permission::Process(tg::grant::permission::process::Permission::Node);
		let principal = owner.cloned().unwrap_or(tg::Principal::Root);
		let authorizations = rows
			.iter()
			.map(|row| tangram_index::authorize::Arg {
				permissions: permission.into(),
				resource: tg::grant::Resource::Id(row.id.clone().into()),
				token: None,
			})
			.collect::<Vec<_>>();
		let authorizations = self
			.server
			.index
			.authorize_batch(&authorizations, &principal)
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the cached processes"))?;
		let authorized = std::iter::zip(rows, authorizations).find_map(|(row, authorization)| {
			authorization
				.is_some_and(|output| output.permissions.contains(permission))
				.then_some(row)
		});
		let Some(Row {
			id,
			error,
			exit,
			output,
			sandbox,
			sandbox_status,
			status,
			stored_at,
		}) = authorized
		else {
			return Ok(ControlFlow::Break(None));
		};

		// If the process failed and the retry flag is set, then return.
		let failed = error.is_some() || exit.is_some_and(|exit| exit != 0);
		if failed && arg.retry {
			return Ok(ControlFlow::Break(None));
		}

		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let token = self.create_process_wait_token(&id, now)?;
		let tti = self
			.server
			.config
			.process
			.time_to_index
			.as_secs()
			.to_i64()
			.unwrap();
		let max_stored_at = now - tti;
		if status.is_finished() && stored_at <= max_stored_at {
			let process = self
				.server
				.index
				.touch_process(&id, now, self.server.config.process.time_to_touch)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to touch the process"))?;
			if process.is_none() {
				return Ok(ControlFlow::Break(None));
			}
		}

		let wait = if status == tg::process::Status::Finished {
			let error = error
				.map(|error| {
					if error.starts_with('{') {
						serde_json::from_str(&error)
							.map(tg::Either::Left)
							.map_err(|error| tg::error!(!error, "failed to deserialize the error"))
					} else {
						error
							.parse()
							.map(tg::Either::Right)
							.map_err(|error| tg::error!(!error, "failed to parse the error id"))
					}
				})
				.transpose()
				.map_err(|error| tg::error!(!error, "invalid error"))?;
			let exit = exit.ok_or_else(|| tg::error!("expected the exit to be set"))?;
			Some(tg::process::wait::Output {
				error,
				exit,
				output,
			})
		} else {
			None
		};

		// If the process is not finished, then create a process lease.
		let lease = if status == tg::process::Status::Finished {
			None
		} else {
			if sandbox_status.is_some_and(|status| status.is_destroyed()) {
				return Ok(ControlFlow::Break(None));
			}

			let statement = formatdoc!(
				"
					update sandboxes
					set heartbeat_at = heartbeat_at
					where id = {p}1 and status != 'destroyed';
				"
			);
			let params = db::params![sandbox.to_string()];
			let result = transaction.execute(statement.into(), params).await;
			let n = crate::database::retry!(result, "failed to execute the statement");
			if n == 0 {
				return Ok(ControlFlow::Break(None));
			}

			let result = self
				.server
				.try_lock_process_with_transaction(transaction, &id)
				.await
				.map_err(|error| tg::error!(!error, "failed to lock the process"))?;
			let status = match result {
				ControlFlow::Break(status) => status,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			let Some(status) = status else {
				return Ok(ControlFlow::Break(None));
			};
			if status.is_finished() {
				return Ok(ControlFlow::Break(None));
			}

			let lease = Self::create_process_lease();
			let statement = formatdoc!(
				"
					insert into process_leases (process, lease)
					values ({p}1, {p}2);
				"
			);
			let params = db::params![id.to_string(), lease];
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to execute the statement");

			// Update lease count.
			let result = self
				.server
				.update_process_lease_count_with_transaction(transaction, &id)
				.await
				.map_err(|error| tg::error!(!error, "failed to update the lease count"))?;
			match result {
				ControlFlow::Break(()) => {},
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}

			Some(lease)
		};

		Ok(ControlFlow::Break(Some(LocalOutput {
			cached: true,
			id: id.clone(),
			lease,
			owner: owner.cloned(),
			process_token: None,
			sandbox: tg::sandbox::create::Output {
				id: sandbox,
				token: None,
			},
			status,
			token,
			wait,
		})))
	}

	async fn try_get_cached_process_with_mismatched_checksum_local(
		&self,
		transaction: &database::Transaction<'_>,
		arg: &tg::process::spawn::Arg,
		host: &str,
		owner: Option<&tg::Principal>,
	) -> tg::Result<ControlFlow<Option<LocalOutput>, database::Error>> {
		let p = transaction.p();

		// If the checksum is not set, then return.
		let Some(expected_checksum) = arg.checksum.clone() else {
			return Ok(ControlFlow::Break(None));
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
			#[cfg(feature = "turso")]
			database::Transaction::Turso(_) => "with params as (select ?1 as command, ?2 as checksum)",
		};
		let is = match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => "is not distinct from",
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(_) => "is",
			#[cfg(feature = "turso")]
			database::Transaction::Turso(_) => "is",
		};
		let checksum_prefix = match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => {
				"split_part(processes.actual_checksum, ':', 1) = split_part(params.checksum, ':', 1)"
			},
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(_) => {
				"split_part(processes.actual_checksum, ':', 1) = split_part(params.checksum, ':', 1)"
			},
			#[cfg(feature = "turso")]
			database::Transaction::Turso(_) => {
				"substr(processes.actual_checksum, 1, instr(processes.actual_checksum || ':', ':') - 1) = substr(params.checksum, 1, instr(params.checksum || ':', ':') - 1)"
			},
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
					processes.status,
					processes.stored_at
				from processes,
				params
				where
					processes.command = params.command and
					processes.cacheable = true and
					processes.error_code {is} 'checksum_mismatch' and
					processes.actual_checksum is not null and
					{checksum_prefix}
				order by processes.created_at desc;
			"
		);
		let params = db::params![arg.command.item.to_string(), expected_checksum.to_string()];
		let rows = {
			let result = transaction
				.query_all_into::<Row>(statement.into(), params)
				.await;
			crate::database::retry!(result, "failed to execute the statement")
		};

		// Authorize.
		let permission =
			tg::grant::Permission::Process(tg::grant::permission::process::Permission::Node);
		let principal = owner.cloned().unwrap_or(tg::Principal::Root);
		let authorizations = rows
			.iter()
			.map(|row| tangram_index::authorize::Arg {
				permissions: permission.into(),
				resource: tg::grant::Resource::Id(row.id.clone().into()),
				token: None,
			})
			.collect::<Vec<_>>();
		let authorizations = self
			.server
			.index
			.authorize_batch(&authorizations, &principal)
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the cached processes"))?;
		let authorized = std::iter::zip(rows, authorizations).find_map(|(row, authorization)| {
			authorization
				.is_some_and(|output| output.permissions.contains(permission))
				.then_some(row)
		});
		let Some(Row {
			actual_checksum,
			depth,
			id: source,
			output,
			sandbox,
			status: source_status,
			stored_at: source_stored_at,
		}) = authorized
		else {
			return Ok(ControlFlow::Break(None));
		};

		let now: i64 = time::OffsetDateTime::now_utc().unix_timestamp();
		let tti = self
			.server
			.config
			.process
			.time_to_index
			.as_secs()
			.to_i64()
			.unwrap();
		let max_stored_at = now - tti;
		if source_status.is_finished() && source_stored_at <= max_stored_at {
			let process = self
				.server
				.index
				.touch_process(&source, now, self.server.config.process.time_to_touch)
				.await
				.map_err(|error| tg::error!(!error, id = %source, "failed to touch the process"))?;
			if process.is_none() {
				return Ok(ControlFlow::Break(None));
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
		let token = self.create_process_wait_token(&id, now)?;

		let status = tg::process::Status::Finished;

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
					lease_count,
					output,
					retry,
					sandbox,
					status,
					stored_at,
					creator,
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
		let (error_data, error_code) = if let Some(error) = &error {
			error
				.store_with_handle(self)
				.await
				.map_err(|error| tg::error!(!error, "failed to store the error"))?;
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
		let creator = &Some(self.context.principal.to_string());
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
			0,
			output.clone().map(db::value::Json),
			arg.retry,
			sandbox.to_string(),
			status.to_string(),
			now,
			creator,
			tty.map(db::value::Json),
		];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

		// Copy the process children.
		let statement = formatdoc!(
			"
				insert into process_children (
					process,
					position,
					cached,
					child,
					options,
					lease
				)
				select
					{p}1,
					position,
					cached,
					child,
					options,
					lease
				from process_children
				where process = {p}2
				on conflict (process, child) do nothing;
			"
		);
		let params = db::params![id.to_string(), source.to_string()];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(Some(LocalOutput {
			cached: true,
			id,
			lease: None,
			owner: owner.cloned(),
			process_token: None,
			sandbox: tg::sandbox::create::Output {
				id: sandbox,
				token: None,
			},
			status,
			token,
			wait: Some(tg::process::wait::Output {
				error: error.as_ref().map(tg::Error::to_data_or_id),
				exit,
				output,
			}),
		})))
	}

	#[allow(clippy::too_many_arguments)]
	async fn create_local_process(
		&self,
		transaction: &database::Transaction<'_>,
		arg: &tg::process::spawn::Arg,
		parent_sandbox: Option<&tg::sandbox::Id>,
		cacheable: bool,
		has_permit: bool,
		host: &str,
		owner: Option<tg::Principal>,
	) -> tg::Result<ControlFlow<LocalOutput, database::Error>> {
		let p = transaction.p();

		// Create an ID.
		let id = tg::process::Id::new();

		// Create a lease.
		let lease = Self::create_process_lease();

		let (sandbox, sandbox_token) = match &arg.sandbox {
			None => return Err(tg::error!("expected the sandbox to be set")),
			Some(tg::Either::Left(sandbox_arg)) => {
				let status = if has_permit {
					tg::sandbox::Status::Started
				} else {
					tg::sandbox::Status::Created
				};
				let sandbox_arg = Self::normalize_sandbox_create_arg(sandbox_arg.clone())?;
				let sandbox = match self
					.create_local_sandbox_with_transaction(
						transaction,
						&sandbox_arg,
						status,
						owner.as_ref(),
					)
					.await?
				{
					ControlFlow::Break(sandbox) => sandbox,
					ControlFlow::Continue(error) => {
						return Ok(ControlFlow::Continue(error));
					},
				};

				// Mint the sandbox token so a runner can authenticate as the sandbox.
				let token = Self::create_sandbox_token_string();
				let statement = formatdoc!(
					"
						insert into sandbox_tokens (sandbox, token)
						values ({p}1, {p}2);
					"
				);
				let params = db::params![sandbox.to_string(), token.clone()];
				let result = transaction.execute(statement.into(), params).await;
				crate::database::retry!(result, "failed to execute the statement");

				(sandbox, Some(token))
			},
			Some(tg::Either::Right(sandbox)) => {
				let result = self
					.try_get_sandbox_with_transaction(transaction, sandbox)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the sandbox"))?;
				let status = match result {
					ControlFlow::Break(status) => status,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				let Some(status) = status else {
					return Err(tg::error!("failed to find the sandbox"));
				};
				if status.is_destroyed() {
					return Err(tg::error!("the sandbox is destroyed"));
				}
				self.authorize_sandbox_spawn(sandbox, parent_sandbox, owner.as_ref())
					.await?;
				(sandbox.clone(), None)
			},
		};

		let status = if has_permit {
			tg::process::Status::Started
		} else {
			tg::process::Status::Created
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
					lease_count,
					retry,
					sandbox,
					started_at,
					status,
					stderr,
					stdin,
					stdout,
					stored_at,
					creator,
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
					{p}19
				);
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let token = self.create_process_wait_token(&id, now)?;
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
		let creator = &Some(self.context.principal.to_string());
		let params = db::params![
			cacheable,
			arg.command.item.to_string(),
			now,
			arg.debug.clone().map(db::value::Json),
			1,
			arg.checksum.as_ref().map(ToString::to_string),
			host,
			id.to_string(),
			0,
			arg.retry,
			sandbox.to_string(),
			started_at,
			status.to_string(),
			(!arg.stderr.is_null()).then(|| arg.stderr.to_string()),
			(!arg.stdin.is_null()).then(|| arg.stdin.to_string()),
			(!arg.stdout.is_null()).then(|| arg.stdout.to_string()),
			now,
			creator,
			tty.map(db::value::Json),
		];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

		// Insert the process lease.
		let statement = formatdoc!(
			"
				insert into process_leases (process, lease)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![id.to_string(), lease];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

		// Update lease count.
		let result = self
			.server
			.update_process_lease_count_with_transaction(transaction, &id)
			.await
			.map_err(|error| tg::error!(!error, "failed to update the lease count"))?;
		match result {
			ControlFlow::Break(()) => {},
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		// Mint the process token so a runner can authenticate as the process.
		let process_token = Server::create_process_token_string();
		let statement = formatdoc!(
			"
				insert into process_tokens (process, token)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![id.to_string(), process_token.clone()];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(LocalOutput {
			cached: false,
			id,
			lease: Some(lease),
			owner,
			process_token: Some(process_token),
			sandbox: tg::sandbox::create::Output {
				id: sandbox,
				token: sandbox_token,
			},
			status,
			token,
			wait: None,
		}))
	}

	async fn create_local_sandbox_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		arg: &tg::sandbox::create::Arg,
		status: tg::sandbox::Status,
		owner: Option<&tg::Principal>,
	) -> tg::Result<ControlFlow<tg::sandbox::Id, database::Error>> {
		let p = transaction.p();
		let id = tg::sandbox::Id::new();
		let creator = &Some(self.context.principal.to_string());
		let statement = formatdoc!(
			r"
				insert into sandboxes (
					id,
					cpu,
					created_at,
					creator,
					heartbeat_at,
					hostname,
					isolation,
					memory,
					mounts,
					network,
					owner,
					started_at,
					status,
					ttl
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
					{p}14
				);
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let heartbeat_at = (status == tg::sandbox::Status::Started).then_some(now);
		let started_at = (status == tg::sandbox::Status::Started).then_some(now);
		let isolation = self.server.resolve_sandbox_isolation()?;
		Server::validate_sandbox_resources(
			&isolation,
			arg.cpu,
			arg.memory,
			arg.hostname.as_deref(),
		)?;
		let cpu = arg
			.cpu
			.map(i64::try_from)
			.transpose()
			.map_err(|error| tg::error!(!error, "invalid sandbox cpu"))?;
		let memory = arg
			.memory
			.map(i64::try_from)
			.transpose()
			.map_err(|error| tg::error!(!error, "invalid sandbox memory"))?;
		let ttl = arg.ttl;
		db::value::DurationSeconds::validate(ttl).map_err(|_| tg::error!("invalid sandbox ttl"))?;
		let params = db::params![
			id.to_string(),
			cpu,
			now,
			creator,
			heartbeat_at,
			arg.hostname.clone(),
			arg.isolation.map(db::value::Json),
			memory,
			(!arg.mounts.is_empty()).then(|| db::value::Json(arg.mounts.clone())),
			arg.network.clone().map(db::value::Json),
			owner.map(ToString::to_string),
			started_at,
			status.to_string(),
			db::value::DurationSeconds(ttl),
		];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(id))
	}

	async fn resolve_spawn_sandbox_owner_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		arg: &tg::process::spawn::Arg,
		parent_sandbox: Option<&tg::sandbox::Id>,
	) -> tg::Result<Option<tg::Principal>> {
		let owner = match arg.sandbox.as_ref() {
			Some(tg::Either::Left(sandbox)) => sandbox.owner.clone(),
			Some(tg::Either::Right(sandbox)) => {
				if let Some(sandbox) = self.server.sandboxes.get(sandbox) {
					return Ok(sandbox.owner().cloned());
				}
				let owner = match Self::try_get_sandbox_owner_with_transaction(transaction, sandbox)
					.await?
				{
					ControlFlow::Break(Some(owner)) => owner,
					ControlFlow::Break(None) => {
						return Err(tg::error!("failed to find the sandbox"));
					},
					ControlFlow::Continue(error) => {
						return Err(tg::error!(!error, "failed to get the sandbox owner"));
					},
				};
				return Ok(owner);
			},
			None => None,
		};
		if let Some(owner) = owner {
			return Ok(Some(owner).filter(|owner| !matches!(owner, tg::Principal::Root)));
		}
		if let Some(parent_sandbox) = parent_sandbox {
			if let Some(sandbox) = self.server.sandboxes.get(parent_sandbox) {
				return Ok(sandbox.owner().cloned());
			}
			match Self::try_get_sandbox_owner_with_transaction(transaction, parent_sandbox).await? {
				ControlFlow::Break(Some(owner)) => return Ok(owner),
				ControlFlow::Break(None) => {},
				ControlFlow::Continue(error) => {
					return Err(tg::error!(!error, "failed to get the parent sandbox owner"));
				},
			}
		}
		let output = if matches!(
			self.context.principal,
			tg::Principal::Anonymous | tg::Principal::Root
		) {
			None
		} else {
			Some(self.context.principal.clone())
		};
		Ok(output)
	}

	async fn authorize_sandbox_spawn(
		&self,
		sandbox: &tg::sandbox::Id,
		parent_sandbox: Option<&tg::sandbox::Id>,
		owner: Option<&tg::Principal>,
	) -> tg::Result<()> {
		let permission =
			tg::grant::Permission::Sandbox(tg::grant::permission::sandbox::Permission::Write);
		let authorized = if matches!(self.context.principal, tg::Principal::Process(_))
			&& parent_sandbox.is_some_and(|parent_sandbox| parent_sandbox == sandbox)
		{
			let principal = owner.cloned().unwrap_or(tg::Principal::Root);
			self.server
				.index
				.authorize(
					tg::grant::Resource::Id(sandbox.clone().into()),
					permission.into(),
					&principal,
				)
				.await
				.map(|output| output.map(|output| output.permissions))
				.map_err(|error| tg::error!(!error, "failed to authorize the sandbox"))?
		} else {
			self.authorize(sandbox.clone(), permission)
				.await
				.map_err(|error| tg::error!(!error, "failed to authorize the sandbox"))?
		};
		if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
			return Err(tg::error!("unauthorized"));
		}
		Ok(())
	}

	async fn try_get_sandbox_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		id: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<Option<tg::sandbox::Status>, database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select status
				from sandboxes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			status: tg::sandbox::Status,
		}
		let result = transaction
			.query_optional_into::<Row>(statement.into(), params)
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(row.map(|row| row.status)))
	}

	async fn try_get_sandbox_owner_with_transaction(
		transaction: &database::Transaction<'_>,
		id: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<Option<Option<tg::Principal>>, database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select owner
				from sandboxes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::value::FromStr>")]
			owner: Option<tg::Principal>,
		}
		let result = transaction
			.query_optional_into::<Row>(statement.into(), params)
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(row.map(|row| row.owner)))
	}

	pub(crate) fn try_acquire_sandbox_permit(
		&self,
		parent_sandbox: Option<&tg::sandbox::Id>,
	) -> Option<crate::sandbox::Permit> {
		if let Some(parent_sandbox) = parent_sandbox
			&& let Some(parent_permit) = self.server.sandbox_permits.get(parent_sandbox)
		{
			let parent_permit = parent_permit.clone();
			if let Ok(guard) = parent_permit.try_lock_owned() {
				return Some(crate::sandbox::Permit(tg::Either::Right(guard)));
			}
		}
		self.server
			.sandbox_semaphore
			.clone()
			.try_acquire_owned()
			.ok()
			.map(|permit| crate::sandbox::Permit(tg::Either::Left(permit)))
	}

	fn spawn_process_parent_permit_task(
		&self,
		parent_sandbox: &tg::sandbox::Id,
		id: &tg::sandbox::Id,
		process: &tg::process::Id,
		process_token: Option<String>,
		token: Option<String>,
	) {
		tokio::spawn({
			let session = self.clone();
			let parent_sandbox = parent_sandbox.clone();
			let sandbox = id.clone();
			let process = process.clone();
			async move {
				let Some(parent_permit) = session
					.server
					.sandbox_permits
					.get(&parent_sandbox)
					.map(|permit| permit.clone())
				else {
					return;
				};
				let permit = parent_permit
					.lock_owned()
					.map(|guard| crate::sandbox::Permit(tg::Either::Right(guard)))
					.await;
				let Ok(started) = session
					.start_sandbox_process(&sandbox, Some(&process))
					.await
					.inspect_err(|error| {
						tracing::trace!(
							error = %error.trace(),
							sandbox = %sandbox,
							process = %process,
							"failed to start the sandbox process with the parent permit"
						);
					})
				else {
					return;
				};
				if !started {
					return;
				}
				session.server.spawn_sandbox_task(SpawnSandboxTaskArg {
					id: sandbox,
					location: tg::Location::Local(tg::location::Local::default()),
					permit,
					process: Some(process),
					process_token,
					started: None,
					token,
				});
			}
		});
	}

	fn spawn_process_scheduler_task(
		&self,
		sandbox: &tg::sandbox::Id,
		process: &tg::process::Id,
		sandbox_arg: Option<tg::sandbox::create::Arg>,
		sandbox_token: Option<String>,
		process_token: Option<String>,
	) {
		let session = self.clone();
		let process = process.clone();
		let sandbox = sandbox.clone();
		self.server
			.dispatch_tasks
			.spawn(|_| async move {
				let result = match sandbox_arg {
					Some(sandbox_arg) => {
						session
							.schedule_process(
								&sandbox,
								sandbox_arg,
								Some(&process),
								sandbox_token,
								process_token,
							)
							.await
					},
					None => {
						session
							.dispatch_process(&sandbox, &process, process_token)
							.await
					},
				};
				if let Err(error) = result {
					session
						.finish_process_after_dispatch_failed(&process, error)
						.await;
				}
			})
			.detach();
	}

	async fn finish_process_after_dispatch_failed(
		&self,
		process: &tg::process::Id,
		error: tg::Error,
	) {
		tracing::error!(%error, %process, "failed to dispatch the process");
		let mut error = error.to_data_or_id();
		if !self.server.config.advanced.internal_error_locations
			&& let tg::Either::Left(error) = &mut error
		{
			error.remove_internal_locations();
		}
		let arg = tg::process::finish::Arg {
			checksum: None,
			error: Some(error),
			exit: 1,
			location: Some(tg::Location::Local(tg::location::Local::default()).into()),
			output: None,
		};
		self.try_finish_process(process, arg)
			.await
			.inspect_err(|error| {
				tracing::error!(%error, %process, "failed to finish the process after dispatch failed");
			})
			.ok();
	}

	async fn add_process_child_creates_cycle_with_transaction(
		transaction: &database::Transaction<'_>,
		parent: &tg::process::Id,
		child: &tg::process::Id,
	) -> tg::Result<ControlFlow<bool, database::Error>> {
		let p = transaction.p();
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
		let result = transaction
			.query_one_value_into::<bool>(statement.into(), params)
			.await;
		let cycle = crate::database::retry!(result, "failed to execute the cycle check");
		Ok(ControlFlow::Break(cycle))
	}

	async fn find_add_process_child_cycle_path_with_transaction(
		transaction: &database::Transaction<'_>,
		parent: &tg::process::Id,
		child: &tg::process::Id,
	) -> tg::Result<ControlFlow<Option<String>, database::Error>> {
		let p = transaction.p();
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
		let result = transaction
			.query_one_value_into::<String>(statement.into(), params)
			.await;
		let cycle = match result {
			Ok(cycle) => Some(cycle),
			Err(error) => {
				if db::Error::is_retry(&error) {
					return Ok(ControlFlow::Continue(error));
				}
				tracing::error!(?error, "failed to get the cycle");
				None
			},
		};
		Ok(ControlFlow::Break(cycle))
	}

	async fn update_parent_depths_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		child: &tg::process::Id,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		let result = match transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(transaction) => self
				.update_parent_depths_postgres(transaction, child.to_string())
				.await
				.map(|result| result.map_continue(database::Error::Postgres)),
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(transaction) => self
				.update_parent_depths_sqlite(transaction, vec![child.to_string()])
				.await
				.map(|result| result.map_continue(database::Error::Sqlite)),
			#[cfg(feature = "turso")]
			database::Transaction::Turso(transaction) => self
				.update_parent_depths_turso(transaction, vec![child.to_string()])
				.await
				.map(|result| result.map_continue(database::Error::Turso)),
		}?;
		Ok(result)
	}

	async fn add_process_child(&self, arg: AddProcessChildArg<'_>) -> tg::Result<()> {
		let child = arg.child.clone();
		let command = arg.command.clone();
		let lease = arg.lease.cloned();
		let options = arg.options.clone();
		let parent = arg.parent.clone();
		let sandbox = arg.sandbox.cloned();
		let session = self.clone();
		self.server
			.process_store
			.run(|transaction| {
				let child = child.clone();
				let lease = lease.clone();
				let options = options.clone();
				let parent = parent.clone();
				let session = session.clone();
				async move {
					session
						.add_process_child_with_transaction(
							transaction,
							&parent,
							arg.cached,
							&child,
							&options,
							lease.as_ref(),
						)
						.await
				}
				.boxed()
			})
			.await
			.map_err(
				|error| tg::error!(!error, %parent, %child, "failed to add the process as a child"),
			)?;

		// Publish the child index task.
		self.spawn_process_child_index_task(&parent, &child, &command, sandbox.as_ref());

		// Publish the child message.
		self.spawn_publish_process_child_message_task(&parent);

		// Wake the watchdog so parent depth changes are observed promptly.
		self.server.spawn_publish_watchdog_message_task();

		Ok(())
	}

	async fn add_process_child_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		parent: &tg::process::Id,
		cached: bool,
		child: &tg::process::Id,
		options: &tg::referent::Options,
		lease: Option<&String>,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		let p = transaction.p();

		// Lock the parent and ensure that it is not finished.
		let result = self
			.server
			.try_lock_process_with_transaction(transaction, parent)
			.await
			.map_err(|error| tg::error!(!error, "failed to lock the parent process"))?;
		let status = match result {
			ControlFlow::Break(status) => status,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let Some(status) = status else {
			return Err(tg::error!("the parent process was not found"));
		};
		if status.is_finished() {
			return Err(tg::error!("the parent process was finished"));
		}

		// Determine if adding this child process creates a cycle.
		let cycle = match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => {
				let result = Self::add_process_child_creates_cycle_with_transaction(
					transaction,
					parent,
					child,
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to check for a process cycle"))?;
				match result {
					ControlFlow::Break(cycle) => cycle,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				}
			},
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(_) => {
				let result = Self::add_process_child_creates_cycle_with_transaction(
					transaction,
					parent,
					child,
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to check for a process cycle"))?;
				match result {
					ControlFlow::Break(cycle) => cycle,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				}
			},
			#[cfg(feature = "turso")]
			database::Transaction::Turso(transaction) => {
				let result = Self::add_process_child_creates_cycle_turso_with_transaction(
					transaction,
					parent,
					child,
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to check for a process cycle"))?;
				match result {
					ControlFlow::Break(cycle) => cycle,
					ControlFlow::Continue(error) => {
						return Ok(ControlFlow::Continue(database::Error::Turso(error)));
					},
				}
			},
		};

		// If adding this child creates a cycle, return an error.
		if cycle {
			// Try to reconstruct the cycle path by walking from the child through its descendants until we find a path back to the parent.
			let cycle = match &transaction {
				#[cfg(feature = "postgres")]
				database::Transaction::Postgres(_) => {
					let result = Self::find_add_process_child_cycle_path_with_transaction(
						transaction,
						parent,
						child,
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to find the process cycle"))?;
					match result {
						ControlFlow::Break(cycle) => cycle,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					}
				},
				#[cfg(feature = "sqlite")]
				database::Transaction::Sqlite(_) => {
					let result = Self::find_add_process_child_cycle_path_with_transaction(
						transaction,
						parent,
						child,
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to find the process cycle"))?;
					match result {
						ControlFlow::Break(cycle) => cycle,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					}
				},
				#[cfg(feature = "turso")]
				database::Transaction::Turso(transaction) => {
					let result = Self::find_add_process_child_cycle_path_turso_with_transaction(
						transaction,
						parent,
						child,
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to find the process cycle"))?;
					match result {
						ControlFlow::Break(cycle) => cycle,
						ControlFlow::Continue(error) => {
							return Ok(ControlFlow::Continue(database::Error::Turso(error)));
						},
					}
				},
			};
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
					lease
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
			lease
		];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

		let result = self
			.update_parent_depths_with_transaction(transaction, child)
			.await
			.map_err(|error| tg::error!(!error, "failed to update parent depths"))?;
		match result {
			ControlFlow::Break(()) => {},
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(()))
	}

	fn spawn_process_child_index_task(
		&self,
		parent: &tg::process::Id,
		child: &tg::process::Id,
		command: &tg::command::Id,
		sandbox: Option<&tg::sandbox::Id>,
	) {
		let arg = tangram_index::process::put::Arg {
			children: None,
			command: command.clone().into(),
			error: None,
			id: child.clone(),
			log: None,
			metadata: tg::process::Metadata::default(),
			output: None,
			parent: Some(parent.clone()),
			sandbox: sandbox.cloned(),
			stored: tangram_index::process::Stored::default(),
			touched_at: time::OffsetDateTime::now_utc().unix_timestamp(),
		};
		let arg = tangram_index::batch::Arg {
			put_processes: vec![arg],
			..Default::default()
		};
		self.server
			.index_tasks
			.spawn(|_| {
				let server = self.server.clone();
				async move {
					let result = server.index.batch(arg).await;
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), "failed to put process to index");
					}
				}
			})
			.detach();
	}

	fn spawn_publish_process_child_message_task(&self, parent: &tg::process::Id) {
		tokio::spawn({
			let session = self.clone();
			let id = parent.clone();
			async move {
				session
					.server
					.messenger
					.publish(format!("processes.{id}.children"), ())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});
	}

	fn create_process_lease() -> String {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
	}

	pub(crate) async fn try_spawn_process_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		let stream = self
			.try_spawn_process(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to spawn the process"))?;

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
