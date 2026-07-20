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
		if matches!(self.context.principal, tg::Principal::Process(_)) {
			return Err(tg::error!("unauthorized"));
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

	pub(crate) async fn try_destroy_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	) -> tg::Result<Option<bool>> {
		let permission =
			tg::grant::Permission::Sandbox(tg::grant::permission::sandbox::Permission::Write);
		let authorize_future = self.authorize(id.clone(), permission);
		let get_future = self.try_get_sandbox_from_index(id);
		let (authorized, sandbox) = future::try_join(authorize_future, get_future).await?;
		if sandbox.is_none()
			|| !authorized.is_some_and(|permissions| permissions.contains(permission))
		{
			return Ok(None);
		}

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
		let destroy_future = self.send_sandbox_control_request(id, request, options);
		let status_future = self.try_get_sandbox_status_local(id);
		let response = match future::select(pin!(destroy_future), pin!(status_future)).await {
			future::Either::Left((response, _)) => response,
			future::Either::Right((status, destroy_future)) => match status? {
				Some(status) if status.is_destroyed() => return Ok(Some(false)),
				Some(_) => destroy_future.await,
				None => return Ok(None),
			},
		};
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
