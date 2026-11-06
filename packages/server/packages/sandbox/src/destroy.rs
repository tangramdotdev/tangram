use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_server_core::{
		Server, Session, location::await_local_future_over_regions_remotes_futures,
	},
};

impl Component {
	/// Stop or destroy the sandboxes that the index records as running on a runner other than this server's, because a single server's restart loses the runners' registrations, so that their heartbeats can no longer expire. Apply the same rule as heartbeat expiration: destroy a sandbox whose time to live is zero, and stop the others.
	pub(crate) async fn expire_unfinished_sandboxes(&self, server: &Server) -> tg::Result<()> {
		let local_runner = server.components.runner.local_runner_id(server);
		let limit = 1024;
		let mut after = None;
		loop {
			// Get the next page of the running sandboxes that are not on this server's runner.
			let ids = server
				.index
				.list_sandbox_ids(after.take(), limit)
				.await
				.map_err(|error| tg::error!(!error, "failed to list the sandboxes"))?;
			let sandboxes = server
				.index
				.try_get_sandboxes(&ids)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the sandboxes"))?;
			let sandboxes = std::iter::zip(&ids, sandboxes)
				.filter_map(|(id, sandbox)| {
					let sandbox = sandbox?;
					let data = sandbox.data?.data;
					let running = !data.status.is_stopped_or_destroyed();
					let remote = sandbox
						.location
						.as_ref()
						.is_some_and(tg::Location::is_remote);
					let local = sandbox.runner.is_some() && sandbox.runner == local_runner;
					(running && !remote && !local).then_some((id.clone(), data.time_to_live))
				})
				.collect::<Vec<_>>();

			// Stop or destroy each sandbox.
			sandboxes
				.into_iter()
				.map(|(id, time_to_live)| async move {
					let error = tg::error::Data {
						code: Some(tg::error::Code::Internal),
						message: Some("server restarted".to_owned()),
						..Default::default()
					};
					let scheduler = &server.components.scheduler;
					let result = if time_to_live.is_some_and(|time_to_live| time_to_live.is_zero())
					{
						scheduler.destroy_runner_sandbox(server, &id, error).await
					} else {
						scheduler.stop_runner_sandbox(server, &id, error).await
					};
					if let Err(error) = result {
						tracing::error!(sandbox = %id, error = %error.trace(), "failed to expire the sandbox");
					}
				})
				.collect::<FuturesUnordered<_>>()
				.collect::<()>()
				.await;

			if ids.len() < limit {
				break;
			}
			after = ids.last().cloned();
		}

		Ok(())
	}

	#[tracing::instrument(name = "sandbox.destroy_request", level = "info", skip_all, fields(sandbox = %id), err(level = "debug"))]
	pub(crate) async fn try_destroy_sandbox(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> tg::Result<Option<bool>> {
		if let Some(output) = self.try_destroy_sandbox_runner(session, id, &arg).await? {
			return Ok(Some(output));
		}
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		let current = locations.local.as_ref().is_some_and(|local| local.current);

		// Destroy the sandbox locally without a search.
		if current
			&& let Some(output) = self
				.try_destroy_sandbox_local(session, id, &arg, false)
				.boxed()
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to destroy the sandbox"))?
		{
			return Ok(Some(output));
		}

		// Search locally while trying the regions and then the remotes.
		let local_future = async {
			if !current {
				return Ok(None);
			}
			self.try_destroy_sandbox_local(session, id, &arg, true)
				.boxed()
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to destroy the sandbox"))
		};
		let regions_and_remotes_future = async {
			if let Some(local) = &locations.local
				&& let Some(output) = self
					.try_destroy_sandbox_regions(session, id, &arg, &local.regions)
					.await
					.map_err(
						|error| tg::error!(!error, %id, "failed to destroy the sandbox in another region"),
					)? {
				return Ok(Some(output));
			}
			self.try_destroy_sandbox_remotes(session, id, &arg, &locations.remotes)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to destroy the sandbox in a remote"),
				)
		};
		let output = await_local_future_over_regions_remotes_futures(
			local_future,
			regions_and_remotes_future,
		)
		.boxed()
		.await?;

		Ok(output)
	}

	async fn try_destroy_sandbox_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
	) -> tg::Result<Option<bool>> {
		let Some(runner) = session
			.server
			.components
			.runner
			.try_get_sandbox_runner(id, arg.location.as_ref())
		else {
			return Ok(None);
		};
		if !self
			.authorize_sandbox_runner(
				session,
				id,
				arg.tokens.local_authorization(),
				tg::authorization::permission::sandbox::Permission::Parent,
			)
			.await?
		{
			return Ok(None);
		}
		let Some((status, control_sender)) = session
			.server
			.components
			.runner
			.state()
			.sandboxes()
			.get(runner.index)
			.map(|sandbox| (sandbox.status, sandbox.control_sender.clone()))
		else {
			return Ok(None);
		};
		if status.is_destroyed() {
			return Ok(Some(false));
		}
		tangram_server_core::checkpoint!(session.server, "sandbox.destroy.runner", sandbox = %id)
			.await;
		let output = self
			.destroy_sandbox_with_control(session, id, arg.error.clone(), Some(control_sender))
			.boxed()
			.await?;
		if arg.wait {
			self.wait_for_sandbox_destroy(session, id).await?;
		}

		Ok(output)
	}

	/// Wait for a sandbox's teardown to finish.
	async fn wait_for_sandbox_destroy(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let wakeups = self
			.create_sandbox_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;
		self.wait_for_sandbox_destroy_local(session, id, wakeups)
			.await?;

		Ok(())
	}

	/// Ask the scheduler that owns a sandbox to destroy it, which returns `None` if the sandbox's runner owns it.
	async fn try_destroy_sandbox_with_scheduler(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		scheduler: &tg::scheduler::Id,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	) -> tg::Result<Option<bool>> {
		let error = match error {
			Some(tg::Either::Left(data)) => Some(data),
			Some(tg::Either::Right(id)) => Some(
				tg::Error::with_id(id)
					.data_with_instance(session)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the sandbox error"))?,
			),
			None => None,
		};
		let arg = tangram_server_scheduler_core::EndSandboxRequestArg {
			error,
			sandbox: id.clone(),
			stop: false,
		};
		let ended = session
			.server
			.components
			.scheduler
			.end_sandbox(session, scheduler, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, %scheduler, "failed to end the sandbox"))?;

		Ok(ended.then_some(true))
	}

	pub(crate) async fn try_destroy_sandbox_local(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
		exhaustive: bool,
	) -> tg::Result<Option<bool>> {
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Parent,
		);
		let resource = tg::Referent::with_node_and_tokens(id.clone(), arg.tokens.clone());
		let authorize_future = session.authorize_with_exhaustive(resource, permission, exhaustive);
		let get_future = self.try_get_sandbox_from_index_with_exhaustive(session, id, exhaustive);
		let (authorized, sandbox) = future::try_join(authorize_future, get_future).await?;
		let authorized = authorized.check_exhaustion()?;
		let Some(sandbox) = sandbox else {
			return Ok(None);
		};
		if sandbox
			.location
			.as_ref()
			.is_some_and(tg::Location::is_remote)
			|| !authorized.permissions.contains(permission)
		{
			return Ok(None);
		}

		// Send the destroy to the sandbox's scheduler if its runner has not connected.
		let status = sandbox.data.as_ref().map(|data| data.data.status);
		let output = match (&sandbox.scheduler, status) {
			(Some(_), Some(tg::sandbox::Status::Destroying)) => Some(false),
			(Some(scheduler), Some(tg::sandbox::Status::Starting)) => {
				self.try_destroy_sandbox_with_scheduler(session, id, scheduler, arg.error.clone())
					.await?
			},
			_ => None,
		};
		if let Some(output) = output {
			if arg.wait {
				self.wait_for_sandbox_destroy(session, id).await?;
			}
			return Ok(Some(output));
		}

		// Destroy a stopped sandbox without its runner, because it has no control connection. Confirm the stop after pending indexing, because a start may not be indexed yet.
		let mut stopped = sandbox
			.data
			.as_ref()
			.is_some_and(|data| data.data.status.is_stopped());
		if stopped {
			session
				.index(tg::index::Arg::default())
				.await?
				.try_last()
				.await?;
			stopped = self
				.try_get_sandbox_from_index(session, id)
				.await?
				.and_then(|sandbox| sandbox.data)
				.is_some_and(|data| data.data.status.is_stopped());
		}
		let running = session
			.server
			.components
			.runner
			.state()
			.sandboxes()
			.get_by_id(id)
			.is_some_and(|state| !state.status.is_stopped_or_destroyed());
		if stopped && !running {
			self.destroy_stopped_sandbox(session, id, arg.error.clone(), sandbox.runner.as_ref())
				.await?;
			if arg.wait {
				self.wait_for_sandbox_destroy(session, id).await?;
			}
			return Ok(Some(true));
		}

		let output = self
			.destroy_sandbox_with_control(session, id, arg.error.clone(), None)
			.boxed()
			.await?;
		if arg.wait && output.is_some() {
			self.wait_for_sandbox_destroy(session, id).await?;
		}

		Ok(output)
	}

	async fn destroy_stopped_sandbox(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
		runner: Option<&tg::runner::Id>,
	) -> tg::Result<()> {
		// Ask the sandbox's runner to destroy it. The runner's report marks the sandbox destroyed, because the runner is the sandbox's source of truth.
		let error = match error {
			Some(tg::Either::Left(data)) => Some(data),
			Some(tg::Either::Right(id)) => Some(
				tg::Error::with_id(id)
					.data_with_instance(session)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the sandbox error"))?,
			),
			None => None,
		};
		let runner = runner.ok_or_else(|| tg::error!(%id, "the sandbox has no runner"))?;
		let destroyed = session
			.server
			.components
			.scheduler
			.destroy_stopped_runner_sandbox(session, runner, id, error)
			.await?;
		if !destroyed {
			return Err(tg::error!(%id, "the sandbox's runner did not destroy the sandbox"));
		}

		Ok(())
	}

	async fn destroy_sandbox_with_control(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
		control_sender: Option<tangram_server_sandbox_core::control::local::Local>,
	) -> tg::Result<Option<bool>> {
		let error = match error {
			Some(tg::Either::Left(data)) => data,
			Some(tg::Either::Right(id)) => tg::Error::with_id(id)
				.data_with_instance(session)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the sandbox error"))?,
			None => tg::error::Data {
				code: Some(tg::error::Code::Cancellation),
				message: Some("the process was canceled".into()),
				..Default::default()
			},
		};
		let request = tg::sandbox::control::ServerRequestArg::Destroy(
			tg::sandbox::control::DestroyServerRequestArg { error: Some(error) },
		);
		let retry = session.server.config.control.request_retry.clone().into();
		let timeout = session.server.config.control.request_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		let runner = control_sender.is_some();
		let response = if let Some(control_sender) = control_sender {
			match control_sender.send_request(request).await {
				Err(error) => Err(error),
				Ok(response) => response.await,
			}
		} else {
			let wakeups = self
				.create_sandbox_status_wakeup_stream(
					session,
					id,
					session.context.stopper.clone(),
					None,
				)
				.await?;
			let destroy_future = async {
				tangram_server_core::checkpoint!(session.server, "sandbox.destroy.control", sandbox = %id).await;
				self.request_sandbox_control(session, id, request, options)
					.boxed()
					.await
			};
			let get_future = self.wait_for_sandbox_destroy_local(session, id, wakeups);
			match future::select(pin!(destroy_future), pin!(get_future)).await {
				future::Either::Left((response, _)) => response,
				future::Either::Right((output, _)) => return output,
			}
		};
		if response.is_err() {
			if runner
				&& session
					.server
					.components
					.runner
					.state()
					.sandboxes()
					.get_by_id(id)
					.is_some_and(|sandbox| sandbox.status.is_destroyed())
			{
				return Ok(Some(false));
			}
			if self
				.try_get_sandbox_from_index(session, id)
				.await?
				.is_some_and(|sandbox| {
					(runner
						|| !sandbox
							.location
							.as_ref()
							.is_some_and(tg::Location::is_remote))
						&& sandbox
							.data
							.as_ref()
							.is_some_and(|output| output.data.status.is_destroyed())
				}) {
				return Ok(Some(false));
			}
		}
		let response = response
			.map_err(
				|error| tg::error!(!error, %id, "failed to send the destroy sandbox control request"),
			)?
			.map_err(
				|error| tg::error!(!error, %id, "the destroy sandbox control request failed"),
			)?;
		let response = response
			.try_unwrap_destroy()
			.map_err(|_| tg::error!(%id, "expected a destroy sandbox response"))?;
		Ok(Some(response.destroyed))
	}

	async fn wait_for_sandbox_destroy_local(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		mut wakeups: futures::stream::BoxStream<'static, ()>,
	) -> tg::Result<Option<bool>> {
		loop {
			let sandbox = self.try_get_sandbox_from_index(session, id).await;
			tangram_server_core::checkpoint!(session.server, "sandbox.destroy.index", sandbox = %id)
				.await;
			let Some(sandbox) =
				sandbox.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox"))?
			else {
				return Ok(None);
			};
			if sandbox
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
			{
				return Ok(None);
			}
			if sandbox
				.data
				.as_ref()
				.is_some_and(|output| output.data.status.is_destroyed())
			{
				return Ok(Some(false));
			}
			if wakeups.next().await.is_none() {
				return Err(tg::error!("the sandbox status wakeup stream ended"));
			}
		}
	}

	async fn try_destroy_sandbox_regions(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
		regions: &[String],
	) -> tg::Result<Option<bool>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_destroy_sandbox_region(session, id, arg, region))
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

	async fn try_destroy_sandbox_region(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
		region: &str,
	) -> tg::Result<Option<bool>> {
		let client = session.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::destroy::Arg {
			location: Some(location.clone().into()),
			tokens: arg.tokens.for_location(&location),
			..arg.clone()
		};
		let Some(destroyed) = client.try_destroy_sandbox(id, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to destroy the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(destroyed))
	}

	async fn try_destroy_sandbox_remotes(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
		remotes: &[tangram_server_core::location::Remote],
	) -> tg::Result<Option<bool>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_destroy_sandbox_remote(session, id, arg, remote))
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

	async fn try_destroy_sandbox_remote(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
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
		let arg = tg::sandbox::destroy::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			tokens: arg.tokens.for_location(&location),
			..arg.clone()
		};
		let Some(destroyed) = client.try_destroy_sandbox(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to destroy the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(destroyed))
	}

	pub(crate) async fn try_destroy_sandbox_request(
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

		let Some(destroyed) = self
			.try_destroy_sandbox(session, &id, arg)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to destroy the sandbox"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		if !destroyed {
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
