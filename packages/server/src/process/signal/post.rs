use {
	crate::Session,
	futures::{StreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_post_process_signal(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<Option<()>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_post_process_signal_local(id, arg.signal, arg.token.as_ref())
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to signal the process"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_post_process_signal_regions(id, arg.signal, &local.regions, arg.token.as_ref())
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to signal the process in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_post_process_signal_remotes(id, arg.signal, &locations.remotes, arg.token.as_ref())
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to signal the process in a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_post_process_signal_local(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<()>> {
		let Some(output) = self
			.try_get_process_local(id, false, token)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
		else {
			return Ok(None);
		};
		let cacheable = output.data.cacheable;

		let permission =
			tg::grant::Permission::Process(tg::grant::permission::process::Permission::Write);
		let resource = tg::Referent::with_item_and_token(id.clone(), token.cloned());
		let authorized = self.authorize(resource, permission).await?;
		if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
			return Ok(None);
		}

		// Check if the process is cacheable.
		if cacheable {
			return Err(tg::error!(%id, "cannot signal cacheable processes"));
		}
		if output.data.status.is_finished() {
			return Ok(Some(()));
		}

		// Send the control request.
		let request = tg::process::control::ServerRequestArg::Signal(
			tg::process::control::SignalServerRequestArg { signal },
		);
		let retry = tangram_futures::retry::Options::default();
		let timeout = if self
			.server
			.config
			.roles
			.contains(&crate::config::Role::Runner)
		{
			self.server.config.runner.stdio_drain_timeout
		} else {
			std::time::Duration::from_secs(10)
		};
		let options = crate::control::Options { retry, timeout };
		let response = self
			.send_process_control_request(id, request, options)
			.await??;
		response
			.try_unwrap_signal()
			.map_err(|_| tg::error!("expected a signal response"))?;

		Ok(Some(()))
	}

	async fn try_post_process_signal_regions(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		regions: &[String],
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<()>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_post_process_signal_region(id, signal, region, token))
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

	async fn try_post_process_signal_region(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		region: &str,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::signal::post::Arg {
			location: Some(location.into()),
			signal,
			token: token.cloned(),
		};
		let Some(()) = client.try_post_process_signal(id, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to signal the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_post_process_signal_remotes(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		remotes: &[crate::location::Remote],
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<()>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_post_process_signal_remote(id, signal, remote, token))
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

	async fn try_post_process_signal_remote(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		remote: &crate::location::Remote,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, %id, "failed to get the remote client"),
		)?;
		let arg = tg::process::signal::post::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			signal,
			token: token.cloned(),
		};
		let Some(()) = client.try_post_process_signal(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to signal the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	pub(crate) async fn try_signal_process_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Post the process signal.
		let Some(()) = self
			.try_post_process_signal(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to post process signal"))?
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
