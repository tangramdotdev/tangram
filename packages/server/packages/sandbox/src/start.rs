use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_server_core::{Session, location::await_local_future_over_regions_remotes_futures},
};

impl Component {
	#[tracing::instrument(name = "sandbox.start_request", level = "info", skip_all, fields(sandbox = %id), err(level = "debug"))]
	pub(crate) async fn try_start_sandbox(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::start::Arg,
	) -> tg::Result<Option<bool>> {
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		let current = locations.local.as_ref().is_some_and(|local| local.current);

		// Start the sandbox locally without a search.
		if current
			&& let Some(output) = self
				.try_start_sandbox_local(session, id, &arg, false)
				.boxed()
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to start the sandbox"))?
		{
			return Ok(Some(output));
		}

		// Search locally while trying the regions and then the remotes.
		let local_future = async {
			if !current {
				return Ok(None);
			}
			self.try_start_sandbox_local(session, id, &arg, true)
				.boxed()
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to start the sandbox"))
		};
		let regions_and_remotes_future = async {
			if let Some(local) = &locations.local
				&& let Some(output) = self
					.try_start_sandbox_regions(session, id, &arg, &local.regions)
					.await
					.map_err(
						|error| tg::error!(!error, %id, "failed to start the sandbox in another region"),
					)? {
				return Ok(Some(output));
			}
			self.try_start_sandbox_remotes(session, id, &arg, &locations.remotes)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to start the sandbox in a remote"))
		};
		let output = await_local_future_over_regions_remotes_futures(
			local_future,
			regions_and_remotes_future,
		)
		.boxed()
		.await?;

		Ok(output)
	}

	async fn try_start_sandbox_local(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::start::Arg,
		exhaustive: bool,
	) -> tg::Result<Option<bool>> {
		// Authorize the start and get the indexed sandbox.
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Parent,
		);
		let resource = tg::Referent::with_node_and_tokens(id.clone(), arg.tokens.clone());
		let authorize_future = session.authorize_with_exhaustive(resource, permission, exhaustive);
		let get_future = self.try_get_sandbox_from_index_with_exhaustive(session, id, exhaustive);
		let (authorized, indexed) = future::try_join(authorize_future, get_future).await?;
		let authorized = authorized.check_exhaustion()?;
		let Some(indexed) = indexed else {
			return Ok(None);
		};
		if indexed
			.location
			.as_ref()
			.is_some_and(tg::Location::is_remote)
			|| !authorized.permissions.contains(permission)
		{
			return Ok(None);
		}

		// Subscribe before reading the state, so that a start or stop between the read and the wait cannot be missed.
		let mut wakeups = self
			.create_sandbox_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;

		// Get the sandbox's state, which its control connection reports while it is running.
		let output = self
			.try_get_sandbox(session, id, tg::sandbox::get::Arg::default())
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
		match output.data.status {
			tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying => {
				return Err(tg::error!(%id, "the sandbox was destroyed"));
			},
			tg::sandbox::Status::Started => return Ok(Some(false)),
			tg::sandbox::Status::Starting => {
				if arg.wait {
					self.wait_for_sandbox_start(session, id, &mut wakeups)
						.await?;
				}
				return Ok(Some(false));
			},
			tg::sandbox::Status::Stopped | tg::sandbox::Status::Stopping => (),
		}

		// Start the next boot on the runner that holds the sandbox's directory.
		let runner = indexed
			.runner
			.ok_or_else(|| tg::error!(%id, "the sandbox has no runner"))?;
		let boot = output.data.boot + 1;
		self.start_sandbox_with_runner(session, id, output, runner, indexed.created_at)
			.await?;

		// Wait for the boot to start if requested, because a connection alone does not identify the boot.
		if arg.wait {
			self.wait_for_sandbox_boot(session, id, boot, &mut wakeups)
				.await?;
		}

		Ok(Some(true))
	}

	async fn start_sandbox_with_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		output: tg::sandbox::get::Output,
		runner: tg::runner::Id,
		created_at: i64,
	) -> tg::Result<()> {
		// Create the sandbox's arg from its data, and a new control token.
		let data = output.data;
		let boot = data.boot + 1;
		let arg = tg::sandbox::create::Arg {
			cpu: data.cpu,
			host: None,
			hostname: data.hostname.clone(),
			location: None,
			memory: data.memory,
			mounts: data.mounts.clone(),
			network: data.network.clone(),
			owner: Some(data.owner.clone()),
			storage: data.storage,
			time_to_live: data.time_to_live,
			time_to_stop: data.time_to_stop,
			wait: false,
		};
		let token = session
			.server
			.components
			.authentication
			.create_sandbox_authentication_token(&session.server, id.clone())?;

		// Enqueue the boot, which the scheduler accepts once its record is durable.
		let request = tangram_server_scheduler_core::EnqueueSandboxRequestArg {
			arg,
			boot,
			created_at,
			creator: data.creator.clone(),
			owner: data.owner.clone(),
			parent: None,
			process: None,
			runner: Some(runner),
			sandbox: id.clone(),
			scheduler: None,
			stop: false,
			token: Some(token),
		};
		session
			.server
			.components
			.scheduler
			.enqueue_sandbox(session, request)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to enqueue the sandbox"))?;

		Ok(())
	}

	pub(crate) async fn wait_for_sandbox_boot(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		boot: u64,
		wakeups: &mut futures::stream::BoxStream<'static, ()>,
	) -> tg::Result<()> {
		let sandboxes = session.server.components.runner.state().sandboxes();
		loop {
			let updated = sandboxes.updated();
			let output = self
				.try_get_sandbox(session, id, tg::sandbox::get::Arg::default())
				.await?
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
			let data = output.data;
			if data.status.is_destroyed() || data.status.is_destroying() {
				let error = match data.error {
					Some(tg::Either::Left(data)) => tg::Error::try_from(data).ok(),
					Some(tg::Either::Right(_)) | None => None,
				};
				let error = if let Some(error) = error {
					tg::error!(!error, %id, "the sandbox was destroyed")
				} else {
					tg::error!(%id, "the sandbox was destroyed")
				};
				return Err(error);
			}
			if data.boot == boot {
				match data.status {
					tg::sandbox::Status::Started => return Ok(()),
					tg::sandbox::Status::Stopped | tg::sandbox::Status::Stopping => {
						let error = match data.error {
							Some(tg::Either::Left(data)) => tg::Error::try_from(data).ok(),
							Some(tg::Either::Right(_)) | None => None,
						};
						let error = if let Some(error) = error {
							tg::error!(!error, %id, "the sandbox failed to start")
						} else {
							tg::error!(%id, "the sandbox failed to start")
						};
						return Err(error);
					},
					tg::sandbox::Status::Destroyed
					| tg::sandbox::Status::Destroying
					| tg::sandbox::Status::Starting => {},
				}
			}
			tokio::select! {
				wakeup = wakeups.next() => {
					if wakeup.is_none() {
						return Err(tg::error!("the sandbox status wakeup stream ended"));
					}
				},
				() = updated => {},
			}
		}
	}

	pub(crate) async fn wait_for_sandbox_start(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		wakeups: &mut futures::stream::BoxStream<'static, ()>,
	) -> tg::Result<()> {
		loop {
			let output = self
				.try_get_sandbox(session, id, tg::sandbox::get::Arg::default())
				.await?
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
			match output.data.status {
				tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying => {
					return Err(tg::error!(%id, "the sandbox was destroyed"));
				},
				tg::sandbox::Status::Started => return Ok(()),
				tg::sandbox::Status::Stopped | tg::sandbox::Status::Stopping => {
					return Err(tg::error!(%id, "the sandbox failed to start"));
				},
				tg::sandbox::Status::Starting => {},
			}
			if wakeups.next().await.is_none() {
				return Err(tg::error!("the sandbox status wakeup stream ended"));
			}
		}
	}

	async fn try_start_sandbox_regions(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::start::Arg,
		regions: &[String],
	) -> tg::Result<Option<bool>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_start_sandbox_region(session, id, arg, region))
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

	async fn try_start_sandbox_region(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::start::Arg,
		region: &str,
	) -> tg::Result<Option<bool>> {
		let client = session.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::start::Arg {
			location: Some(location.clone().into()),
			tokens: arg.tokens.for_location(&location),
			wait: arg.wait,
		};
		let Some(started) = client
			.try_start_sandbox(id, arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to start the sandbox"))?
		else {
			return Ok(None);
		};
		Ok(Some(started))
	}

	async fn try_start_sandbox_remotes(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::start::Arg,
		remotes: &[tangram_server_core::location::Remote],
	) -> tg::Result<Option<bool>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_start_sandbox_remote(session, id, arg, remote))
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

	async fn try_start_sandbox_remote(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::start::Arg,
		remote: &tangram_server_core::location::Remote,
	) -> tg::Result<Option<bool>> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, %id, "failed to get the remote client"),
			)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let arg = tg::sandbox::start::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			tokens: arg.tokens.for_location(&location),
			wait: arg.wait,
		};
		let Some(started) = client.try_start_sandbox(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to start the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(started))
	}

	pub(crate) async fn try_start_sandbox_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let id = id
			.parse::<tg::sandbox::Id>()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the sandbox id"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		let Some(started) = self
			.try_start_sandbox(session, &id, arg)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to start the sandbox"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		if !started {
			return Ok(http::Response::builder()
				.status(http::StatusCode::CONFLICT)
				.empty()
				.unwrap()
				.boxed_body());
		}

		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();

		Ok(response)
	}
}
