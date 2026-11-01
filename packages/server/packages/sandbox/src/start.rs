use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_server_core::Session,
};

/// The failure of a start, which is unassigned when the start never reached the sandbox's runner.
enum StartError {
	Assigned(tg::Error),
	Unassigned(tg::Error),
}

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

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_start_sandbox_local(session, id, &arg)
					.boxed()
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to start the sandbox"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_start_sandbox_regions(session, id, &arg, &local.regions)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to start the sandbox in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_start_sandbox_remotes(session, id, &arg, &locations.remotes)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to start the sandbox in a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_start_sandbox_local(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::start::Arg,
	) -> tg::Result<Option<bool>> {
		// Authorize the start and get the indexed sandbox.
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Parent,
		);
		let resource = tg::Referent::with_node_and_tokens(id.clone(), arg.tokens.clone());
		let authorize_future = session.authorize(resource, permission);
		let get_future = self.try_get_sandbox_from_index(session, id);
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
			tg::sandbox::Status::Destroyed => {
				return Err(tg::error!(%id, "the sandbox was destroyed"));
			},
			tg::sandbox::Status::Started => return Ok(Some(false)),
			tg::sandbox::Status::Starting => {
				self.wait_for_sandbox_start(session, id, &mut wakeups)
					.await?;
				return Ok(Some(false));
			},
			tg::sandbox::Status::Stopped | tg::sandbox::Status::Stopping => (),
		}

		// Start the next boot on the runner that holds the sandbox's directory.
		let runner = indexed
			.runner
			.ok_or_else(|| tg::error!(%id, "the sandbox has no runner"))?;
		self.start_sandbox_with_runner(session, id, output, runner, indexed.created_at)
			.await?;

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
			ttl: data.ttl,
		};
		let token = session
			.server
			.components
			.authentication
			.create_sandbox_authentication_token(&session.server, id.clone())?;
		let mut wakeups = self
			.create_sandbox_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;
		let connection_future = self.subscribe_sandbox_connection(session, id).await?;

		// Record the start concurrently with enqueuing it, because the index orders the write before the runner's report of the boot.
		let starting = tg::sandbox::Data {
			boot,
			error: None,
			status: tg::sandbox::Status::Starting,
			usage: None,
			..data.clone()
		};
		let index_future =
			self.put_started_sandbox(session, id, starting, runner.clone(), created_at);
		let request = tangram_server_scheduler_core::EnqueueSandboxRequestArg {
			arg,
			boot,
			creator: data.creator.clone(),
			owner: data.owner.clone(),
			parent: None,
			process: None,
			runner: Some(runner.clone()),
			sandbox: id.clone(),
			scheduler: None,
			token: Some(token),
		};
		let enqueue_future = session
			.server
			.components
			.scheduler
			.enqueue_sandbox(session, request);
		let (index_result, enqueue_result) = future::join(index_future, enqueue_future).await;
		if let Err(error) = index_result {
			tracing::warn!(error = %error.trace(), sandbox = %id, "failed to record the starting sandbox");
		}

		// Wait for the boot to start.
		let result = async {
			let scheduler = enqueue_result.map_err(|error| {
				StartError::Unassigned(tg::error!(!error, %id, "failed to enqueue the sandbox"))
			})?;
			let timeout = session.server.config.scheduler.create_sandbox_timeout;
			let heartbeat = session
				.server
				.components
				.scheduler
				.scheduler_heartbeat_expired(session, &scheduler);
			let boot_future = self.wait_for_sandbox_boot(session, id, boot, &mut wakeups);

			// Fail if the scheduler discards the start, and otherwise wait for the boot, because a connection alone does not identify the boot.
			let discarded_future = async {
				connection_future.await?;
				future::pending::<tg::Result<()>>().await
			};
			tokio::select! {
				result = discarded_future => result.map_err(StartError::Unassigned),
				result = tokio::time::timeout(timeout, boot_future) => {
					let result = result
						.map_err(|_| tg::error!(%id, "timed out waiting for the sandbox to start"))
						.and_then(std::convert::identity);
					result.map_err(StartError::Assigned)
				},
				result = heartbeat => {
					let error = match result {
						Ok(()) => tg::error!(%id, %scheduler, "the scheduler heartbeat expired"),
						Err(error) => error,
					};
					Err(StartError::Assigned(error))
				},
			}
		}
		.await;

		// Record the sandbox as stopped with the error if the start never reached its runner. Otherwise the runner reports the outcome, because it is the sandbox's source of truth.
		match result {
			Ok(()) => {},
			Err(StartError::Assigned(error)) => return Err(error),
			Err(StartError::Unassigned(error)) => {
				let stopped = tg::sandbox::Data {
					boot,
					error: Some(error_data(session, &error)),
					status: tg::sandbox::Status::Stopped,
					usage: None,
					..data
				};
				if let Err(error) = self
					.put_started_sandbox(session, id, stopped, runner, created_at)
					.await
				{
					tracing::warn!(error = %error.trace(), sandbox = %id, "failed to record the failed start");
				}
				return Err(error);
			},
		}

		Ok(())
	}

	async fn put_started_sandbox(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		data: tg::sandbox::Data,
		runner: tg::runner::Id,
		created_at: i64,
	) -> tg::Result<()> {
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let data = tg::sandbox::get::Output {
			data,
			location: Some(location.clone()),
			tokens: tg::authorization::Tokens::default(),
		};
		let account = session
			.server
			.components
			.usage
			.usage_account(session, &data.data.owner)
			.await?;
		let put_sandbox = tangram_index::sandbox::put::Arg {
			account,
			created_at,
			data: Some(data),
			id: id.clone(),
			location: Some(location),
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			processes: None,
			runner: Some(runner),
			touched_at: session.server.clock.unix_timestamp(),
		};
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(put_sandbox)],
		};
		Self::spawn_publish_sandbox_status_task(&session.server, id);
		session
			.server
			.components
			.indexer
			.index_batch(&session.server, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put the sandbox in the index"))?;
		Ok(())
	}

	async fn wait_for_sandbox_boot(
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
			if data.status.is_destroyed() {
				return Err(tg::error!(%id, "the sandbox was destroyed"));
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
					tg::sandbox::Status::Destroyed | tg::sandbox::Status::Starting => {},
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

	async fn wait_for_sandbox_start(
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
				tg::sandbox::Status::Destroyed => {
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
		};
		let Some(stoped) = client
			.try_start_sandbox(id, arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to start the sandbox"))?
		else {
			return Ok(None);
		};
		Ok(Some(stoped))
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
		};
		let Some(stoped) = client.try_start_sandbox(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to start the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(stoped))
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

		let Some(stoped) = self
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
		if !stoped {
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

fn error_data(session: &Session, error: &tg::Error) -> tg::Either<tg::error::Data, tg::error::Id> {
	let mut error = error.to_data_or_id();
	if !session.server.config.advanced.internal_error_locations
		&& let tg::Either::Left(error) = &mut error
	{
		error.remove_internal_locations();
	}
	error
}
