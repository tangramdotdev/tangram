use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream::BoxStream},
	num::ToPrimitive as _,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

mod cached;
mod child;
mod grant;
mod lease;
mod local;
mod start;
mod wait;

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
			async move |_| match session
				.try_spawn_process_task(arg, parent_sandbox, &progress)
				.boxed()
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

		let stream = progress.stream().attach(task).boxed();

		Ok(stream)
	}

	async fn try_spawn_process_task(
		&self,
		arg: tg::process::spawn::Arg,
		parent_sandbox: Option<tg::sandbox::Id>,
		progress: &crate::progress::Handle<Option<tg::process::spawn::Output>>,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let location = self.server.location(arg.location.as_ref())?;
		let runner_matches_location = self
			.server
			.config
			.roles
			.contains(&crate::config::Role::Runner)
			&& {
				let config = &self.server.config.runner;
				let runner_location = config.remote.as_ref().map_or_else(
					|| tg::Location::Local(tg::location::Local::default()),
					|name| {
						tg::Location::Remote(tg::location::Remote {
							name: name.clone(),
							region: None,
						})
					},
				);
				runner_location == location
			};
		let new_sandbox = matches!(arg.sandbox, Some(tg::Either::Left(_)));
		let requested =
			if runner_matches_location && let Some(tg::Either::Left(sandbox)) = &arg.sandbox {
				let scheduler = &self.server.config.scheduler;
				Some(tg::runner::Capacity {
					cpus: sandbox.cpu.unwrap_or(scheduler.default_cpu),
					memory: sandbox.memory.unwrap_or(scheduler.default_memory),
				})
			} else {
				None
			};
		let allocation = if runner_matches_location && new_sandbox && arg.cached != Some(true) {
			self.try_acquire_sandbox_capacity(parent_sandbox.as_ref(), requested.unwrap())
		} else {
			None
		};
		let shortcut = runner_matches_location
			&& arg.cached != Some(true)
			&& (!new_sandbox || allocation.is_some());

		let mut output = if shortcut {
			self.try_spawn_process_local(arg.clone(), parent_sandbox, allocation, Some(&location))
				.boxed()
				.await?
		} else {
			let spawn_future = match location.clone() {
				tg::Location::Local(tg::location::Local { region: None }) => self
					.try_spawn_process_local(arg.clone(), parent_sandbox.clone(), None, None)
					.boxed(),
				tg::Location::Local(tg::location::Local {
					region: Some(region),
				}) => self
					.try_spawn_process_region(arg.clone(), progress, region)
					.boxed(),
				tg::Location::Remote(tg::location::Remote {
					name: remote,
					region,
				}) => self
					.try_spawn_process_remote(arg.clone(), progress, remote, region)
					.boxed(),
			};
			if runner_matches_location
				&& new_sandbox
				&& arg.cached != Some(true)
				&& let Some(parent) = &arg.parent
				&& let Some(parent_sandbox) = &parent_sandbox
			{
				let notify_future = self.spawn_process_notify_borrowable_capacity(
					parent,
					parent_sandbox,
					requested.unwrap(),
				);
				match futures::future::select(spawn_future, pin!(notify_future)).await {
					futures::future::Either::Left((result, _)) => result?,
					futures::future::Either::Right((result, spawn_future)) => {
						if let Err(error) = result {
							tracing::debug!(
								error = %error.trace(),
								%parent,
								"failed to notify the scheduler of borrowable capacity"
							);
						}
						spawn_future.await?
					},
				}
			} else {
				spawn_future.await?
			}
		};
		if runner_matches_location
			&& let Some(output) = &mut output
			&& !output.cached
		{
			output.location = Some(location);
		}
		let mut lease_guard = output
			.as_ref()
			.and_then(|output| lease::LeaseGuard::new(self, output));
		if let Some(output) = &output {
			self.spawn_process_add_child(&arg, output).await?;
		}
		if let Some(guard) = &mut lease_guard {
			guard.disarm();
		}

		Ok(output)
	}

	async fn try_spawn_process_local(
		&self,
		arg: tg::process::spawn::Arg,
		parent_sandbox: Option<tg::sandbox::Id>,
		allocation: Option<crate::runner::Allocation>,
		cache_location: Option<&tg::Location>,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		// Authorize the command if a process may be created.
		if arg.cached != Some(true) {
			self.spawn_process_authorize_command(&arg).await?;
		}

		// Determine whether the process is cacheable.
		let cacheable = Self::spawn_process_is_cacheable(&arg);

		// Authorize the sandbox owner.
		self.spawn_process_authorize_sandbox_owner(&arg).await?;

		// Get or create the local process.
		let mut output = self
			.spawn_process_get_or_create_local_process(&arg, parent_sandbox.as_ref(), cacheable)
			.boxed()
			.await?;
		if matches!(arg.sandbox, Some(tg::Either::Left(_)))
			&& let Some(output) = &mut output
			&& !output.cached
		{
			output.allocation = allocation;
		}
		if cache_location.is_some_and(tg::Location::is_remote)
			&& let Some(output) = &mut output
			&& !output.cached
		{
			output.process_token = None;
			output.sandbox_token = None;
			output.token = None;
		}
		let output = if cacheable && arg.cached.is_none() {
			self.spawn_process_start_or_get_cached(&arg, output, cache_location)
				.boxed()
				.await?
		} else {
			// Start the local process.
			let output = self.spawn_process_start_local(&arg, output).boxed().await?;

			// Wait for the local process or get a cached process.
			self.spawn_process_wait_or_get_cached(&arg, output, cacheable)
				.boxed()
				.await?
		};
		let Some(mut output) = output else {
			return Ok(None);
		};
		let mut lease_guard = lease::LeaseGuard::new(self, &output);

		// Add tokens to the local output.
		self.spawn_process_add_tokens(&mut output)?;

		// Create a public grant if necessary.
		self.spawn_process_create_public_grant_if_requested(&arg, &output)
			.await?;
		if let Some(guard) = &mut lease_guard {
			guard.disarm();
		}

		Ok(Some(output))
	}

	pub(super) fn create_process_wait_token(
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
		self.spawn_process_push_command(&arg.command, Some(location.clone()), progress)
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
		self.spawn_process_push_command(&arg.command, Some(destination), progress)
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
		command: &tg::Referent<tg::command::Id>,
		location: Option<tg::Location>,
		progress: &crate::progress::Handle<Option<tg::process::spawn::Output>>,
	) -> tg::Result<()> {
		let push_arg = tg::push::Arg {
			commands: true,
			destination: location,
			items: vec![tg::Referent::with_item_and_token(
				tg::Either::Left(command.item.clone().into()),
				command.token().cloned(),
			)],
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
