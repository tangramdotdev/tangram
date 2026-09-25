use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
};

impl Session {
	pub(crate) async fn try_destroy_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> tg::Result<Option<bool>> {
		if let Some(output) = self.try_destroy_sandbox_runner(id, &arg).await? {
			return Ok(Some(output));
		}
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_destroy_sandbox_local(id, arg.error.clone())
					.boxed()
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to destroy the sandbox"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_destroy_sandbox_regions(id, &arg, &local.regions)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to destroy the sandbox in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_destroy_sandbox_remotes(id, &arg, &locations.remotes)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to destroy the sandbox in a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_destroy_sandbox_runner(
		&self,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
	) -> tg::Result<Option<bool>> {
		let Some(runner) = self.try_get_sandbox_runner_inner(id, arg.location.as_ref()) else {
			return Ok(None);
		};
		if !self
			.authorize_sandbox_runner(
				id,
				&[],
				tg::authorization::permission::sandbox::Permission::Write,
			)
			.await?
		{
			return Ok(None);
		}
		let Some((status, control_sender)) = self
			.server
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
		crate::checkpoint!(self.server, "sandbox.destroy.runner", sandbox = %id).await;
		self.destroy_sandbox_with_control(id, arg.error.clone(), Some(control_sender))
			.boxed()
			.await
	}

	pub(crate) async fn try_destroy_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	) -> tg::Result<Option<bool>> {
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Write,
		);
		let authorize_future = self.authorize(id.clone(), permission);
		let get_future = self.try_get_sandbox_from_index(id);
		let (authorized, sandbox) = future::try_join(authorize_future, get_future).await?;
		if sandbox.is_none_or(|sandbox| {
			sandbox
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
		}) || !authorized.is_some_and(|permissions| permissions.contains(permission))
		{
			return Ok(None);
		}

		self.destroy_sandbox_with_control(id, error, None)
			.boxed()
			.await
	}

	async fn destroy_sandbox_with_control(
		&self,
		id: &tg::sandbox::Id,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
		control_sender: Option<super::control::local::Local>,
	) -> tg::Result<Option<bool>> {
		let error = match error {
			Some(tg::Either::Left(data)) => data,
			Some(tg::Either::Right(id)) => tg::Error::with_id(id)
				.data_with_handle(self)
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
		let options = crate::control::Options {
			retry: tangram_futures::retry::Options::default(),
			timeout: std::time::Duration::from_secs(10),
		};
		let runner = control_sender.is_some();
		let response = if let Some(control_sender) = control_sender {
			match control_sender.send_request(request).await {
				Err(error) => Err(error),
				Ok(response) => response.await,
			}
		} else {
			let wakeups = self
				.create_sandbox_status_wakeup_stream(id, self.context.stopper.clone(), None)
				.await?;
			let destroy_future = async {
				crate::checkpoint!(self.server, "sandbox.destroy.control", sandbox = %id).await;
				self.request_sandbox_control(id, request, options)
					.boxed()
					.await
			};
			let get_future = self.wait_for_sandbox_destroy_local(id, wakeups);
			match future::select(pin!(destroy_future), pin!(get_future)).await {
				future::Either::Left((response, _)) => response,
				future::Either::Right((output, _)) => return output,
			}
		};
		if response.is_err() {
			if runner
				&& self
					.server
					.runner
					.state()
					.sandboxes()
					.get_by_id(id)
					.is_some_and(|sandbox| sandbox.status.is_destroyed())
			{
				return Ok(Some(false));
			}
			if self
				.try_get_sandbox_from_index(id)
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
		id: &tg::sandbox::Id,
		mut wakeups: futures::stream::BoxStream<'static, ()>,
	) -> tg::Result<Option<bool>> {
		loop {
			let sandbox = self.try_get_sandbox_from_index(id).await;
			crate::checkpoint!(self.server, "sandbox.destroy.index", sandbox = %id).await;
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
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
		regions: &[String],
	) -> tg::Result<Option<bool>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_destroy_sandbox_region(id, arg, region))
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
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
		region: &str,
	) -> tg::Result<Option<bool>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::destroy::Arg {
			location: Some(location.into()),
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
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<bool>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_destroy_sandbox_remote(id, arg, remote))
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
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<bool>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, %id, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::destroy::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
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

	pub(crate) async fn destroy_sandbox_when_available(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
		connection_future: super::ConnectionFuture,
	) -> tg::Result<()> {
		match self.try_destroy_sandbox(id, arg.clone()).await {
			Ok(Some(_)) => return Ok(()),
			Ok(None) => {},
			Err(error) => {
				tracing::error!(
					error = %error.trace(),
					%id,
					"failed to destroy the sandbox before its control connection"
				);
			},
		}
		let timeout = self.server.config.scheduler.create_sandbox_timeout;
		tokio::time::timeout(timeout, connection_future)
			.await
			.map_err(|_| {
				tg::error!(
					%id,
					"timed out waiting for the sandbox control connection before destroying it"
				)
			})??;
		let output = self
			.try_destroy_sandbox(id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to destroy the sandbox"))?;
		if output.is_none() {
			return Err(tg::error!(
				%id,
				"failed to find the sandbox after its control connection"
			));
		}

		Ok(())
	}

	pub(crate) async fn try_destroy_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let id = id
			.parse::<tg::sandbox::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the sandbox id"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		let Some(destroyed) = self
			.try_destroy_sandbox(&id, arg)
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
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();

		Ok(response)
	}
}
