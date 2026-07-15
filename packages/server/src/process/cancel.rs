use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub async fn try_cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<Option<()>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_cancel_process_local(id, arg.clone())
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to cancel the process"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_cancel_process_regions(id, arg.clone(), &local.regions)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to cancel the process in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_cancel_process_remotes(id, arg, &locations.remotes)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to cancel the process in a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_cancel_process_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<Option<()>> {
		if let Some(result) = self.server.runner.state.try_update_process(id, |process| {
			if process.data.status.is_finished() {
				return Err(tg::error!("the process is already finished"));
			}
			if !process.leases.remove(&arg.lease) {
				return Err(tg::error!("the process lease was not found"));
			}
			if process.leases.is_empty() {
				process.stopper.stop();
			}
			Ok(())
		}) {
			result?;
			return Ok(Some(()));
		}
		let request = tg::process::control::ServerRequestArg::ReleaseLease(
			tg::process::control::ReleaseLeaseServerRequestArg { lease: arg.lease },
		);
		let options = crate::control::Options {
			retry: tangram_futures::retry::Options::default(),
			timeout: std::time::Duration::from_secs(10),
		};
		let session = self.server.session(&self.server.context);
		let release_future = self
			.send_process_control_request(id, request, options)
			.boxed();
		let get_future = session.try_get_process_local(id, false, None).boxed();
		let response = match future::select(pin!(release_future), pin!(get_future)).await {
			future::Either::Left((response, _)) => response,
			future::Either::Right((process, release_future)) => {
				let Some(process) = process
					.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
				else {
					return Ok(None);
				};
				if process.data.status.is_finished() {
					return Err(tg::error!("the process is already finished"));
				}
				release_future.await
			},
		};
		let response = response
			.map_err(|error| tg::error!(!error, %id, "failed to release the process lease"))?
			.map_err(|error| tg::error!(!error, %id, "the release process lease request failed"))?;
		response
			.try_unwrap_release_lease()
			.map_err(|_| tg::error!("expected a release process lease response"))?;
		Ok(Some(()))
	}

	async fn try_cancel_process_regions(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
		regions: &[String],
	) -> tg::Result<Option<()>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_cancel_process_region(id, arg.clone(), region))
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

	async fn try_cancel_process_region(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
		region: &str,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::cancel::Arg {
			location: Some(location.into()),
			..arg
		};
		let Some(()) = client.try_cancel_process(id, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to cancel the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_cancel_process_remotes(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<()>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_cancel_process_remote(id, arg.clone(), remote))
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

	async fn try_cancel_process_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, %id, "failed to get the remote client"),
		)?;
		let arg = tg::process::cancel::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			..arg
		};
		let Some(()) = client.try_cancel_process(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to cancel the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	pub(crate) async fn try_cancel_process_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Parse the ID.
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

		// Parse the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("query parameters required"))?;

		let Some(()) = self
			.try_cancel_process(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to cancel the process"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the response.
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
