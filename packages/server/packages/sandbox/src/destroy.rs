use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_index::prelude::*,
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

		// Create the request's ID, which the passes without and with a search share, so that the holder acts on the request once.
		let request_id = tangram_server_core::control::id();

		// Destroy the sandbox locally without a search.
		if current
			&& let Some(output) = self
				.try_destroy_sandbox_local(session, id, &request_id, &arg, false)
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
			self.try_destroy_sandbox_local(session, id, &request_id, &arg, true)
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

		// Destroy a stopped sandbox through the index, which holds it, so that a start of the sandbox cannot race with the destroy.
		if status.is_stopped() {
			return Ok(None);
		}
		tangram_server_core::checkpoint!(session.server, "sandbox.destroy.runner", sandbox = %id)
			.await;
		let error = self
			.get_sandbox_destroy_error(session, arg.error.clone())
			.await?;
		self.destroy_sandbox_with_runner(session, id, error, control_sender, arg.wait)
			.boxed()
			.await
	}

	pub(crate) async fn try_destroy_sandbox_local(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		request_id: &str,
		arg: &tg::sandbox::destroy::Arg,
		exhaustive: bool,
	) -> tg::Result<Option<bool>> {
		// Authorize the destroy.
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Parent,
		);
		let resource = tg::Referent::with_node_and_tokens(id.clone(), arg.tokens.clone());
		let authorized = session
			.authorize_with_exhaustive(resource, permission, exhaustive)
			.await?
			.check_exhaustion()?;
		if !authorized.permissions.contains(permission) {
			return Ok(None);
		}

		// Subscribe before sending the request, so that the destroy cannot be missed.
		let mut wakeups = self
			.create_sandbox_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;

		// Send the request to the sandbox's holder until the holder destroys the sandbox, or this destroy records the destruction of a stopped sandbox, which the index holds.
		let error = self
			.get_sandbox_destroy_error(session, arg.error.clone())
			.await?;
		let mut next_request_id = request_id.to_owned();
		let destroyed = loop {
			// Use the call's request ID for the first request, and a new one for each later request, which asks again after the sandbox changed.
			let request_id =
				std::mem::replace(&mut next_request_id, tangram_server_core::control::id());
			let request = tg::sandbox::control::ServerRequestArg::Destroy(
				tg::sandbox::control::DestroyServerRequestArg {
					error: error.clone(),
				},
			);
			let retry = session.server.config.control.request_retry.clone().into();
			let timeout = session.server.config.control.request_timeout;
			let options = tangram_server_core::control::Options { retry, timeout };
			let holder_arg = crate::holder::Arg {
				checkpoint: Some("sandbox.destroy"),
				deadline: session.server.control_read_deadline(),
				exhaustive,
				fallback: false,
				id: request_id,
				options,
				request,
				wakeups: Some(&mut wakeups),
			};
			let output = self
				.request_sandbox_holder(session, id, holder_arg, |_| false)
				.boxed()
				.await?
				.ok_or_else(|| tg::error!("the sandbox status wakeup stream ended"))?;

			// A sandbox in a remote is not destroyed here.
			if output
				.indexed
				.as_ref()
				.and_then(|sandbox| sandbox.location.as_ref())
				.is_some_and(tg::Location::is_remote)
			{
				return Ok(None);
			}

			// A destroyed sandbox has nothing left to destroy, a stopped sandbox has no holder but the index, and a sandbox that neither the index nor a holder has does not exist.
			let data = output
				.indexed
				.as_ref()
				.and_then(|sandbox| sandbox.data.as_ref())
				.map(|data| &data.data);
			let status = data.map(|data| data.status);
			let destroying = data.is_some_and(crate::holder::is_destroying_stopped);
			match (output.reply, status) {
				(Some(Ok(tg::sandbox::control::ClientResponseOutput::Destroy(output))), _) => {
					break output.destroyed;
				},
				(Some(Ok(_)), _) => {
					return Err(tg::error!(%id, "expected a destroy sandbox response"));
				},
				(_, Some(tg::sandbox::Status::Destroyed)) => break false,

				// Remove the directory of a sandbox that an earlier destroy recorded as destroying while it was stopped, because that destroy's removal may not have finished.
				(_, Some(tg::sandbox::Status::Destroying)) if destroying => {
					let runner = output.indexed.and_then(|sandbox| sandbox.runner);
					if let Some(runner) = runner {
						self.spawn_remove_stopped_sandbox_task(session, id, error.clone(), runner);
					}
					break false;
				},

				(_, Some(tg::sandbox::Status::Stopped)) => {
					let indexed = output.indexed.unwrap();
					if self
						.try_destroy_stopped_sandbox(session, id, error.clone(), indexed)
						.await?
					{
						break true;
					}

					// Destroy the sandbox through its holder, because a start reserved its next boot.
				},
				(Some(Err(error)), _) => {
					return Err(
						tg::error!(!error, %id, "the destroy sandbox control request failed"),
					);
				},
				(None, _) => return Ok(None),
			}
		};

		// Wait for the sandbox to be destroyed if requested.
		if arg.wait {
			self.wait_for_sandbox_destroy(session, id, &mut wakeups)
				.await?;
		}

		Ok(Some(destroyed))
	}

	/// Get the error that a destroy records, if the destroy has one.
	async fn get_sandbox_destroy_error(
		&self,
		session: &Session,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	) -> tg::Result<Option<tg::error::Data>> {
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

		Ok(error)
	}

	/// Destroy a sandbox that this server's runner holds.
	async fn destroy_sandbox_with_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		error: Option<tg::error::Data>,
		control_sender: tangram_server_sandbox_core::control::local::Local,
		wait: bool,
	) -> tg::Result<Option<bool>> {
		// Subscribe before sending the request, so that the destroy cannot be missed.
		let mut wakeups = self
			.create_sandbox_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;

		// Send the request to the runner.
		let request = tg::sandbox::control::ServerRequestArg::Destroy(
			tg::sandbox::control::DestroyServerRequestArg { error },
		);
		let response = match control_sender.send_request(request).await {
			Err(error) => Err(error),
			Ok(response) => response.await,
		};
		let destroyed = || {
			session
				.server
				.components
				.runner
				.state()
				.sandboxes()
				.get_by_id(id)
				.is_some_and(|sandbox| sandbox.status.is_destroyed())
		};
		let destroyed = match response {
			// Destroy the sandbox through its holder if this server's runner no longer holds it.
			Ok(Ok(std::ops::ControlFlow::Continue(_))) => return Ok(None),
			Ok(Ok(std::ops::ControlFlow::Break(
				tg::sandbox::control::ClientResponseOutput::Destroy(output),
			))) => output.destroyed,
			Ok(Ok(std::ops::ControlFlow::Break(_))) => {
				return Err(tg::error!(%id, "expected a destroy sandbox response"));
			},
			Ok(Err(_)) | Err(_) if destroyed() => false,
			Ok(Err(error)) => {
				return Err(tg::error!(!error, %id, "the destroy sandbox control request failed"));
			},
			Err(error) => {
				return Err(
					tg::error!(!error, %id, "failed to send the destroy sandbox control request"),
				);
			},
		};

		// Wait for the sandbox to be destroyed if requested.
		if wait {
			self.wait_for_sandbox_destroy(session, id, &mut wakeups)
				.await?;
		}

		Ok(Some(destroyed))
	}

	/// Destroy a sandbox that the index records as stopped. Record the destruction in the index with a write that is rejected unless the sandbox is still stopped at its boot, so that of a start and a destroy only one wins, and return `false` if a start reserved the next boot. Then remove the sandbox's directory on the runner that has it, whose report marks the sandbox destroyed. A sandbox that was stopped before it was placed has no directory, so the write records it as destroyed.
	async fn try_destroy_stopped_sandbox(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		error: Option<tg::error::Data>,
		indexed: tangram_index::sandbox::Sandbox,
	) -> tg::Result<bool> {
		// Create the record of the destruction.
		let mut data = indexed
			.data
			.ok_or_else(|| tg::error!(%id, "expected the sandbox data"))?;
		let stopped_boot = data.data.boot;
		let runner = indexed.runner;
		data.data.status = if runner.is_some() {
			tg::sandbox::Status::Destroying
		} else {
			tg::sandbox::Status::Destroyed
		};
		data.tokens = tg::authorization::Tokens::default();
		let now = session.server.clock.unix_timestamp();
		let put_arg = tangram_index::sandbox::put::Arg {
			account: indexed.account,
			created_at: indexed.created_at,
			data: Some(data),
			id: id.clone(),
			location: indexed.location,
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			process: None,
			processes: None,
			registration: None,
			runner: runner.clone(),
			stopped_boot: Some(stopped_boot),
			stopped_usage: None,
			touched_at: now,
		};

		// Record the destruction, unless a start reserved the next boot first.
		tangram_server_core::checkpoint!(session.server, "sandbox.destroy.reserve", sandbox = %id)
			.await;
		let items = vec![tangram_index::Item::PutSandbox(Box::new(put_arg))];
		let write_arg = tangram_index::write::Arg { items };
		let recorded = session
			.server
			.index
			.write(write_arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to record the sandbox's destruction"))?
			.is_ok();
		if !recorded {
			return Ok(false);
		}
		Self::spawn_publish_sandbox_status_task(&session.server, id);

		// Remove the sandbox's directory.
		if let Some(runner) = runner {
			self.spawn_remove_stopped_sandbox_task(session, id, error, runner);
		}

		Ok(true)
	}

	/// Remove the directory of a stopped sandbox that the index records as destroying, retrying until the runner that has the directory removes it.
	fn spawn_remove_stopped_sandbox_task(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		error: Option<tg::error::Data>,
		runner: tg::runner::Id,
	) {
		let component = self.clone();
		let session = session.server.session(&session.server.context);
		let id = id.clone();
		let retry = tangram_futures::retry::Options {
			max_retries: u64::MAX,
			..session.server.config.control.request_retry.clone().into()
		};
		tokio::spawn(async move {
			let mut retries = std::pin::pin!(tangram_futures::retry::stream(retry));
			while retries.next().await.is_some() {
				let result = component
					.remove_stopped_sandbox(&session, &id, error.clone(), &runner)
					.await;
				let Err(error) = result else {
					return;
				};
				tracing::warn!(error = %error.trace(), sandbox = %id, %runner, "failed to remove the stopped sandbox's directory");

				// Stop once the sandbox is destroyed, because another destroy removed the directory.
				let destroyed = session
					.server
					.index
					.try_get_sandbox(&id)
					.await
					.ok()
					.flatten()
					.and_then(|sandbox| sandbox.data)
					.is_some_and(|data| data.data.status.is_destroyed());
				if destroyed {
					return;
				}
			}
		});
	}

	/// Ask the runner of a stopped sandbox to remove its directory. The runner's report marks the sandbox destroyed.
	async fn remove_stopped_sandbox(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		error: Option<tg::error::Data>,
		runner: &tg::runner::Id,
	) -> tg::Result<()> {
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

	/// Wait for a sandbox's teardown to finish.
	async fn wait_for_sandbox_destroy(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		wakeups: &mut futures::stream::BoxStream<'static, ()>,
	) -> tg::Result<()> {
		loop {
			let sandbox = self
				.try_get_sandbox_from_index(session, id)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox"))?;
			let destroyed = sandbox.is_none_or(|sandbox| {
				sandbox
					.location
					.as_ref()
					.is_some_and(tg::Location::is_remote)
					|| sandbox
						.data
						.as_ref()
						.is_some_and(|data| data.data.status.is_destroyed())
			});
			if destroyed {
				return Ok(());
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
