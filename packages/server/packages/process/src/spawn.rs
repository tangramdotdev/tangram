use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture, stream::BoxStream},
	num::ToPrimitive as _,
	std::{collections::BTreeMap, pin::pin},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::Session,
	tracing::Instrument as _,
};

mod cached;
mod child;
mod local;
mod permissions;
mod push;
mod sandbox;
mod wait;

pub(super) mod lease;

pub(super) struct PrepareOutput {
	/// The stdio streams that a connect declared, which the runner of a scheduled process opens when the process starts.
	pub open: Option<crate::connect::service::Open>,

	pub parent_sandbox: Option<tg::sandbox::Id>,
}

impl Component {
	pub async fn try_spawn_process(
		&self,
		session: &Session,
		mut arg: tg::process::spawn::Arg,
	) -> tg::Result<
		BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>,
	> {
		let prepare_output = self.spawn_process_prepare(session, &mut arg).await?;
		self.try_spawn_process_inner(session, arg, prepare_output)
			.await
	}

	pub(super) async fn spawn_process_prepare(
		&self,
		session: &Session,
		arg: &mut tg::process::spawn::Arg,
	) -> tg::Result<PrepareOutput> {
		if matches!(session.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		if arg.sandbox.is_none() {
			return Err(tg::error!(
				"unsandboxed processes cannot be spawned on the server"
			));
		}
		let request_origin = session
			.server
			.try_get_request_origin_sandbox(session.context.origin)?
			.map(|sandbox| {
				(
					sandbox.id.clone(),
					sandbox
						.create
						.host
						.clone()
						.unwrap_or_else(|| tg::host::current().to_owned()),
				)
			});
		let (request_origin_sandbox, sandbox_host) = request_origin.unzip();
		if let Some(origin) = &request_origin_sandbox
			&& let Some(tg::Either::Right(target)) = &arg.sandbox
			&& &target.node != origin
		{
			return Err(tg::error!(
				%origin,
				target = %target.node,
				"the target sandbox does not match the request origin sandbox"
			));
		}

		if let Some(tg::Either::Right(sandbox)) = &arg.sandbox {
			arg.location = sandbox
				.options
				.location
				.clone()
				.map(Into::into)
				.or_else(|| arg.location.clone());
		}

		// Get the authenticated process.
		let authenticated_process = match &session.context.principal {
			tg::Principal::Process(process) => {
				session.try_get_authenticated_process(process).await?
			},
			_ => None,
		};

		// Inherit the host from the authenticated parent without resolving a destination default.
		if let tg::Either::Left(command) = &mut arg.command.node
			&& command.host.is_none()
		{
			command.host = authenticated_process
				.as_ref()
				.map(|process| process.host.clone())
				.or(sandbox_host);
		}

		// If the authentication is from a process, then update the parent, location, and retry.
		if let Some(process) = &authenticated_process
			&& let tg::Principal::Process(id) = &session.context.principal
		{
			arg.debug = process.debug.clone();
			arg.parent = Some(id.clone());
			arg.location = process.location.clone().map(Into::into);
			arg.retry = process.retry;
		}

		// Validate an unchecksummed child sandbox against its parent.
		if arg.checksum.is_none()
			&& let Some(parent) = request_origin_sandbox.as_ref().or_else(|| {
				authenticated_process
					.as_ref()
					.map(|process| &process.sandbox)
			}) && let Some(tg::Either::Left(sandbox)) = &arg.sandbox
		{
			session
				.server
				.components
				.sandbox
				.validate_sandbox_create_arg_with_parent(session, sandbox, parent)
				.await?;
		}

		// Get the parent sandbox if there is one.
		let parent_sandbox = request_origin_sandbox.or_else(|| {
			authenticated_process
				.as_ref()
				.map(|process| process.sandbox.clone())
		});

		let output = PrepareOutput {
			open: None,
			parent_sandbox,
		};

		Ok(output)
	}

	fn spawn_process_resolve_command(
		session: &Session,
		arg: &mut tg::process::spawn::Arg,
	) -> tg::Result<tg::Referent<tg::command::Id>> {
		let id = match &mut arg.command.node {
			tg::Either::Left(command_arg) => {
				let host = command_arg
					.host
					.clone()
					.or_else(|| session.server.config.process.spawn.host.clone())
					.unwrap_or_else(|| tg::host::current().to_owned());
				command_arg.host = Some(host.clone());
				let command = tg::process::data::Command::new(command_arg.clone(), host);
				command.id()?
			},
			tg::Either::Right(id) => id.clone(),
		};
		let options = arg.command.options.clone();
		let command = tg::Referent::new(id, options);

		Ok(command)
	}

	pub(super) async fn try_spawn_process_inner(
		&self,
		session: &Session,
		arg: tg::process::spawn::Arg,
		prepare_output: PrepareOutput,
	) -> tg::Result<
		BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>,
	> {
		let PrepareOutput {
			open,
			parent_sandbox,
		} = prepare_output;

		// Create the progress.
		let capacity = session.server.config.progress.channel_capacity;
		let progress = tg::progress::Handle::new(capacity);

		// Spawn the task.
		let task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
			let progress = progress.clone();
			let span = tracing::Span::current();
			move |_| {
				async move {
					match component
						.try_spawn_process_task(&session, arg, parent_sandbox, open, &progress)
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
				}
				.instrument(span)
			}
		});

		let stream = progress
			.stream(session.server.config.progress.interval)
			.attach(task)
			.boxed();

		Ok(stream)
	}

	#[tracing::instrument(name = "process.schedule", level = "info", skip_all, fields(parent = arg.parent.as_ref().map(ToString::to_string), process = tracing::field::Empty, process_index = tracing::field::Empty), err(level = "debug"))]
	async fn try_spawn_process_task(
		&self,
		session: &Session,
		mut arg: tg::process::spawn::Arg,
		parent_sandbox: Option<tg::sandbox::Id>,
		open: Option<crate::connect::service::Open>,
		progress: &tg::progress::Handle<Option<tg::process::spawn::Output>>,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let location = session.server.location(arg.location.as_ref())?;
		tracing::info!(location = %location, parent = arg.parent.as_ref().map(ToString::to_string), "scheduling the process");
		let runner_matches_location =
			Self::spawn_process_runner_matches_location(session, &location);
		let new_sandbox = matches!(arg.sandbox, Some(tg::Either::Left(_)));

		// Resolve the resources of a new sandbox that this server's runner may create, and get the capacity that it reserves.
		let requested =
			if runner_matches_location && let Some(tg::Either::Left(sandbox)) = &mut arg.sandbox {
				let defaults = session.server.config.scheduler.default_capacity();
				tangram_server_core::sandbox::resolve_sandbox_resources(&defaults, sandbox);
				Some(tangram_server_core::sandbox::sandbox_capacity(sandbox))
			} else {
				None
			};
		let has_parent = arg.parent.is_some() && parent_sandbox.is_some();
		let shortcut_allowed = !location.is_remote() || has_parent;
		let allocation = if shortcut_allowed
			&& runner_matches_location
			&& new_sandbox
			&& arg.cached != Some(true)
		{
			Self::try_acquire_sandbox_capacity(session, parent_sandbox.as_ref(), requested.unwrap())
		} else {
			None
		};
		let shortcut = shortcut_allowed
			&& runner_matches_location
			&& arg.cached != Some(true)
			&& (!new_sandbox || allocation.is_some());
		let notify = if shortcut {
			None
		} else {
			self.try_prepare_spawn_process_for_location(
				session,
				&mut arg,
				&location,
				parent_sandbox.as_ref(),
			)
			.await
		};

		let mut output = if shortcut {
			self.try_spawn_process_local(
				session,
				arg.clone(),
				parent_sandbox,
				allocation,
				None,
				Some(&location),
			)
			.boxed()
			.await?
		} else {
			let spawn_future = match location.clone() {
				tg::Location::Local(tg::location::Local {
					region: Some(region),
				}) if Some(region.as_str()) != session.server.config.region.as_deref() => self
					.try_spawn_process_region(session, arg.clone(), progress, region)
					.boxed(),
				tg::Location::Local(_) => self
					.try_spawn_process_local(
						session,
						arg.clone(),
						parent_sandbox.clone(),
						None,
						open,
						None,
					)
					.boxed(),
				tg::Location::Remote(tg::location::Remote {
					name: remote,
					region,
				}) => self
					.try_spawn_process_remote(session, arg.clone(), progress, remote, region)
					.boxed(),
			};
			if let Some(notify) = notify {
				match futures::future::select(spawn_future, notify).await {
					futures::future::Either::Left((result, _)) => result?,
					futures::future::Either::Right(((), spawn_future)) => spawn_future.await?,
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
			.and_then(|output| lease::LeaseGuard::new(session, output));
		if let Some(output) = &output {
			match &output.process {
				tg::Either::Left(index) => {
					tracing::Span::current().record("process_index", index);
					tracing::info!(
						cached = output.cached,
						parent = arg.parent.as_ref().map(ToString::to_string),
						process_index = index,
						"scheduled the process"
					);
				},
				tg::Either::Right(id) => {
					tracing::Span::current().record("process", tracing::field::display(id));
					tracing::info!(cached = output.cached, parent = arg.parent.as_ref().map(ToString::to_string), process = %id, "scheduled the process");
				},
			}
			self.spawn_process_add_child(session, &arg, output).await?;
		}
		if let Some(guard) = &mut lease_guard {
			guard.disarm();
		}

		Ok(output)
	}

	pub(super) async fn try_prepare_spawn_process_for_location(
		&self,
		session: &Session,
		arg: &mut tg::process::spawn::Arg,
		location: &tg::Location,
		parent_sandbox: Option<&tg::sandbox::Id>,
	) -> Option<BoxFuture<'static, ()>> {
		let Some(tg::Either::Left(sandbox)) = &arg.sandbox else {
			return None;
		};
		let parent_sandbox = parent_sandbox?;
		if !Self::spawn_process_runner_matches_location(session, location) {
			return None;
		}
		arg.scheduler = Some(session.server.runner.state().wait_for_scheduler().await);
		let parent = arg.parent.as_ref()?;
		if arg.cached == Some(true) {
			return None;
		}
		let requested = tangram_server_core::sandbox::sandbox_capacity(sandbox);
		let component = self.clone();
		let session = session.clone();
		let parent = parent.clone();
		let parent_sandbox = parent_sandbox.clone();
		let notify = async move {
			if let Err(error) = component.spawn_process_notify_borrowable_capacity(&session, &parent, &parent_sandbox, requested).await {
				tracing::debug!(error = %error.trace(), %parent, "failed to notify the scheduler of borrowable capacity");
			}
		}.boxed();
		Some(notify)
	}

	#[must_use]
	pub(super) fn spawn_process_runner_matches_location(
		session: &Session,
		location: &tg::Location,
	) -> bool {
		if !session
			.server
			.config
			.roles
			.contains(&tangram_server_core::config::Role::Runner)
		{
			return false;
		}
		let config = &session.server.config.runner;
		let runner_location = config.remote.as_ref().map_or_else(
			|| tg::Location::Local(tg::location::Local::default()),
			|name| {
				tg::Location::Remote(tg::location::Remote {
					name: name.clone(),
					region: None,
				})
			},
		);
		match location {
			tg::Location::Local(local) if config.remote.is_none() => local
				.region
				.as_deref()
				.is_none_or(|region| Some(region) == session.server.config.region.as_deref()),
			_ => runner_location == *location,
		}
	}

	async fn try_spawn_process_local(
		&self,
		session: &Session,
		mut arg: tg::process::spawn::Arg,
		parent_sandbox: Option<tg::sandbox::Id>,
		allocation: Option<tangram_server_core::runner::capacity::Allocation>,
		open: Option<crate::connect::service::Open>,
		cache_location: Option<&tg::Location>,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let command = Self::spawn_process_resolve_command(session, &mut arg)?;

		// Determine whether the process is cacheable.
		let cacheable = Self::spawn_process_is_cacheable(&arg);
		let write_command_permissions = cache_location.is_none_or(|location| !location.is_remote());

		// Authorize the sandbox owner.
		self.spawn_process_authorize_sandbox_owner(session, &arg)
			.await?;

		// Get or create the local process.
		let mut output = self
			.spawn_process_get_or_create_local_process(
				session,
				&arg,
				&command,
				parent_sandbox.as_ref(),
				cacheable,
				write_command_permissions,
			)
			.boxed()
			.await?;
		if matches!(arg.sandbox, Some(tg::Either::Left(_)))
			&& let Some(output) = &mut output
			&& !output.cached
		{
			output.allocation = allocation;
			output.open = open;
		}
		if cache_location.is_some_and(tg::Location::is_remote)
			&& let Some(output) = &mut output
			&& !output.cached
		{
			output.process_token = None;
			output.sandbox_token = None;
			output.tokens.clear();
		}
		let output = if cacheable && arg.cached.is_none() {
			self.spawn_process_in_sandbox_or_get_cached(
				session,
				&arg,
				&command,
				output,
				cache_location,
			)
			.boxed()
			.await?
		} else {
			// Spawn the process in a sandbox.
			let output = self
				.spawn_process_in_new_or_existing_sandbox(session, &arg, output, None)
				.boxed()
				.await?;

			// Wait for the local process or get a cached process.
			self.spawn_process_wait_or_get_cached(session, &arg, output, cacheable)
				.boxed()
				.await?
		};
		let Some(mut output) = output else {
			return Ok(None);
		};
		let mut lease_guard = lease::LeaseGuard::new(session, &output);

		output.command = Some(command.node);

		// Add tokens to the local output.
		Self::spawn_process_add_tokens(session, &mut output)?;

		// Create a public grant if necessary.
		self.spawn_process_create_public_grant_if_requested(session, &arg, &output)
			.await?;
		if let Some(guard) = &mut lease_guard {
			guard.disarm();
		}

		Ok(Some(output))
	}

	pub(super) fn create_process_wait_token(
		session: &Session,
		id: &tg::process::Id,
		now: i64,
	) -> tg::Result<Option<tg::authorization::Token>> {
		let expires_at = now
			+ session
				.server
				.config
				.process
				.permission_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		session.create_token(
			id.clone().into(),
			vec![tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Parent,
			)],
			expires_at,
		)
	}

	async fn try_spawn_process_region(
		&self,
		session: &Session,
		mut arg: tg::process::spawn::Arg,
		progress: &tg::progress::Handle<Option<tg::process::spawn::Output>>,
		region: String,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let client = session
			.get_region_session_for_process(&region)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the region client"),
			)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		self.spawn_process_push_command(session, &mut arg.command, &location)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to push the command"))?;
		let mut arg = tg::process::spawn::Arg {
			location: Some(location.clone().into()),
			..arg
		};
		Self::update_spawn_process_arg_for_location(&mut arg, &location);
		let stream = client
			.try_spawn_process(arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to spawn the process"))?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.next().await {
			let event = event.and_then(|event| {
				event.try_map_output(|output| {
					output
						.map(|mut output| -> tg::Result<_> {
							Self::update_spawn_process_output_referents_for_location(
								session,
								&mut output,
								&location,
								false,
							)?;
							Ok(output)
						})
						.transpose()
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
		session: &Session,
		mut arg: tg::process::spawn::Arg,
		progress: &tg::progress::Handle<Option<tg::process::spawn::Output>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let client = session
			.get_remote_session_for_process(&remote)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
			)?;
		let trusted = client.trusted();
		let destination = tg::Location::Remote(tg::location::Remote {
			name: remote.clone(),
			region: region.clone(),
		});
		self.spawn_process_push_command(session, &mut arg.command, &destination)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to push the command"))?;
		let mut arg = tg::process::spawn::Arg {
			location: Some(
				tg::Location::Local(tg::location::Local {
					region: region.clone(),
				})
				.into(),
			),
			..arg
		};
		Self::update_spawn_process_arg_for_location(&mut arg, &destination);
		let stream = client
			.try_spawn_process(arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to spawn the process"))?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.next().await {
			let remote = remote.clone();
			let event = event.and_then(|event| {
				event.try_map_output(|output| {
					output
						.map(|mut output| -> tg::Result<_> {
							let region = match output.location.take() {
								Some(tg::Location::Local(local)) => local.region,
								Some(tg::Location::Remote(remote)) => remote.region,
								None => None,
							};
							let location = tg::Location::Remote(tg::location::Remote {
								name: remote.clone(),
								region,
							});
							Self::update_spawn_process_output_referents_for_location(
								session,
								&mut output,
								&location,
								trusted,
							)?;
							Ok(output)
						})
						.transpose()
				})
			});
			if let Some(output) = progress.forward(event) {
				return Ok(output);
			}
		}
		Err(tg::error!("expected an output"))
	}

	pub(super) fn spawn_process_command_nodes(
		command: &tg::Referent<tg::Either<tg::process::spawn::CommandArg, tg::command::Id>>,
	) -> tg::Result<Vec<tg::Referent<tg::Id>>> {
		let nodes = match &command.node {
			tg::Either::Left(command) => Self::spawn_process_command_referents(command)?
				.into_iter()
				.map(|referent| referent.map(Into::into))
				.collect(),
			tg::Either::Right(id) => vec![tg::Referent::new(
				id.clone().into(),
				command.options.clone(),
			)],
		};
		Ok(nodes)
	}

	fn spawn_process_command_referents(
		command: &tg::process::spawn::CommandArg,
	) -> tg::Result<Vec<tg::Referent<tg::object::Id>>> {
		let builder = tg::command::Builder::try_with_spawn_arg(command.clone())?;
		let mut referents = BTreeMap::<tg::object::Id, tg::referent::Options>::new();
		for object in builder.objects() {
			let referent = object.to_referent();
			let options = referents.entry(referent.node).or_default();
			if options.location.is_none() {
				options.location = referent.options.location;
			}
			options.tokens.inherit(&referent.options.tokens);
		}
		let referents = referents
			.into_iter()
			.map(|(id, options)| tg::Referent::new(id, options))
			.collect();

		Ok(referents)
	}

	pub(super) fn update_spawn_process_arg_for_location(
		arg: &mut tg::process::spawn::Arg,
		location: &tg::Location,
	) {
		Self::update_spawn_process_command_for_location(&mut arg.command, location);
		if let Some(tg::Either::Right(sandbox)) = &mut arg.sandbox {
			sandbox.options.tokens = sandbox.options.tokens.for_location(location);
			sandbox.options.location = arg
				.location
				.as_ref()
				.and_then(tg::location::Arg::to_location);
		}
	}

	pub(super) fn update_spawn_process_command_for_location(
		command: &mut tg::Referent<tg::Either<tg::process::spawn::CommandArg, tg::command::Id>>,
		location: &tg::Location,
	) {
		command.options.tokens = command.options.tokens.for_location(location);
		let tg::Either::Left(command) = &mut command.node else {
			return;
		};
		*command = command.clone().for_location(location);
	}

	pub(super) fn update_spawn_process_output_referents_for_location(
		session: &Session,
		output: &mut tg::process::spawn::Output,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		session.update_tokens_and_location(
			&mut output.tokens,
			Some(&mut output.location),
			location,
			trusted,
		)?;
		if let Some(outcome) = &mut output.outcome {
			Self::update_outcome_referents_for_location(session, outcome, location, trusted)?;
		}
		Ok(())
	}

	pub(crate) async fn try_spawn_process_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		let stream = self
			.try_spawn_process(session, arg)
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
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
