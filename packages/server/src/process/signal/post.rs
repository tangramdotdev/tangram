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
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		mut arg: tg::process::signal::post::Arg,
	) -> tg::Result<Option<()>> {
		let mut process = process.clone();
		if self
			.try_resolve_process(&mut process, &mut arg.location, Some(&mut arg.tokens), &[])
			.await?
			.is_none()
		{
			return Ok(None);
		}
		let id = process.clone().right();
		let process = &process;
		if let Some(id) = &id
			&& let Some(control) = self
				.try_get_process_control_runner(
					id,
					arg.location.as_ref(),
					&arg.tokens,
					tg::authorization::permission::process::Set::PARENT,
				)
				.await?
		{
			return self
				.post_process_signal_with_control(
					id,
					control.data,
					arg.signal,
					Some(control.control_sender),
				)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to signal the process"));
		}
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(id) = &id
				&& let Some(output) = self
					.try_post_process_signal_local(id, arg.signal, arg.tokens.local_authorization())
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to signal the process"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_post_process_signal_regions(process, arg.signal, &local.regions, &arg.tokens)
				.await
				.map_err(
					|error| tg::error!(!error, %process, "failed to signal the process in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_post_process_signal_remotes(process, arg.signal, &locations.remotes, &arg.tokens)
			.await
			.map_err(
				|error| tg::error!(!error, %process, "failed to signal the process in a remote"),
			)? {
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(in crate::process) async fn try_post_process_signal_local(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		tokens: &[tg::authorization::Token],
	) -> tg::Result<Option<()>> {
		let Some(output) = self
			.try_get_process_local(id, false, false, tokens, tg::process::Source::Auto)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
		else {
			return Ok(None);
		};
		if output
			.location
			.as_ref()
			.is_some_and(tg::Location::is_remote)
		{
			return Ok(None);
		}
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent,
		);
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let authorized = self
			.authorize(resource, permission)
			.await?
			.check_exhaustion()?;
		if !authorized.permissions.contains(permission) {
			return Ok(None);
		}
		self.post_process_signal_with_control(id, output.data, signal, None)
			.await
	}

	async fn post_process_signal_with_control(
		&self,
		id: &tg::process::Id,
		data: tg::process::Data,
		signal: tg::process::Signal,
		control_sender: Option<crate::process::control::local::Local>,
	) -> tg::Result<Option<()>> {
		// Check if the process is cacheable.
		if data.cacheable {
			return Err(tg::error!(%id, "cannot signal cacheable processes"));
		}
		if data.status.is_finished() {
			return Ok(Some(()));
		}

		// Send the control request.
		let request = tg::process::control::ServerRequestArg::Signal(
			tg::process::control::SignalServerRequestArg { signal },
		);
		let retry = self.server.config.control.request_retry.clone().into();
		let timeout = if self
			.server
			.config
			.roles
			.contains(&crate::config::Role::Runner)
		{
			self.server.config.runner.stdio_drain_timeout
		} else {
			self.server.config.control.request_timeout
		};
		let options = crate::control::Options { retry, timeout };
		let response = if let Some(control_sender) = control_sender {
			control_sender.request(request).await?
		} else {
			self.request_process_control(id, request, options).await??
		};
		response
			.try_unwrap_signal()
			.map_err(|_| tg::error!("expected a signal response"))?;

		Ok(Some(()))
	}

	async fn try_post_process_signal_regions(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		signal: tg::process::Signal,
		regions: &[String],
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<()>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_post_process_signal_region(process, signal, region, tokens))
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
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		signal: tg::process::Signal,
		region: &str,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_session_for_process(region).await.map_err(
			|error| tg::error!(!error, region = %region, %process, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::signal::post::Arg {
			location: Some(location.clone().into()),
			signal,
			tokens: tokens.for_location(&location),
		};
		let Some(()) = client.try_post_process_signal(process, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to signal the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_post_process_signal_remotes(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		signal: tg::process::Signal,
		remotes: &[crate::location::Remote],
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<()>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_post_process_signal_remote(process, signal, remote, tokens))
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
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		signal: tg::process::Signal,
		remote: &crate::location::Remote,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<()>> {
		let client = self
			.get_remote_session_for_process(&remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, %process, "failed to get the remote client"),
			)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let arg = tg::process::signal::post::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			signal,
			tokens: tokens.for_location(&location),
		};
		let Some(()) = client.try_post_process_signal(process, arg).await.map_err(
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
		process: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Parse the process.
		let process = process
			.parse::<tg::Either<tg::process::Nonce, tg::process::Id>>()
			.map_err(|_| tg::error!(argument, "failed to parse the process"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Post the process signal.
		let Some(()) = self
			.try_post_process_signal(&process, arg)
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
