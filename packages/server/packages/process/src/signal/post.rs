use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::{Session, location::await_local_future_over_regions_remotes_futures},
};

impl Component {
	pub(crate) async fn try_post_process_signal(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		mut arg: tg::process::signal::post::Arg,
	) -> tg::Result<Option<()>> {
		let mut process = process.clone();
		if self
			.try_resolve_process(
				session,
				&mut process,
				&mut arg.location,
				Some(&mut arg.tokens),
				&[],
			)
			.await?
			.is_none()
		{
			return Ok(None);
		}
		let id = process.clone().right();
		let process = &process;
		// Signal the process if this server's runner holds it.
		if let Some(id) = &id
			&& let Some(output) = self
				.try_post_process_signal_runner(session, id, &arg)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to signal the process"))?
		{
			return Ok(Some(output));
		}
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		let current = locations.local.as_ref().is_some_and(|local| local.current);

		// Create the request's ID, which the passes without and with a search share, so that the holder acts on the request once.
		let request_id = tangram_server_core::control::id();

		// Signal the process locally without a search.
		if current
			&& let Some(id) = &id
			&& let Some(output) = self
				.try_post_process_signal_local(
					session,
					id,
					&request_id,
					arg.signal,
					arg.tokens.local_authorization(),
					false,
				)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to signal the process"))?
		{
			return Ok(Some(output));
		}

		// Search locally while trying the regions and then the remotes.
		let local_future = async {
			let (true, Some(id)) = (current, &id) else {
				return Ok(None);
			};
			self.try_post_process_signal_local(
				session,
				id,
				&request_id,
				arg.signal,
				arg.tokens.local_authorization(),
				true,
			)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to signal the process"))
		};
		let regions_and_remotes_future = async {
			if let Some(local) = &locations.local
				&& let Some(output) = self
					.try_post_process_signal_regions(
						session,
						process,
						arg.signal,
						&local.regions,
						&arg.tokens,
					)
					.await
					.map_err(
						|error| tg::error!(!error, %process, "failed to signal the process in another region"),
					)? {
				return Ok(Some(output));
			}
			self.try_post_process_signal_remotes(
				session,
				process,
				arg.signal,
				&locations.remotes,
				&arg.tokens,
			)
			.await
			.map_err(
				|error| tg::error!(!error, %process, "failed to signal the process in a remote"),
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

	async fn try_post_process_signal_runner(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: &tg::process::signal::post::Arg,
	) -> tg::Result<Option<()>> {
		let Some(control) = self
			.try_get_process_control_runner(
				session,
				id,
				arg.location.as_ref(),
				&arg.tokens,
				tg::authorization::permission::process::Set::PARENT,
			)
			.await?
		else {
			return Ok(None);
		};
		if control.data.cacheable {
			return Err(tg::error!(%id, "cannot signal cacheable processes"));
		}
		if control.data.status.is_finished() {
			return Ok(Some(()));
		}

		// Send the request to the runner. A runner that no longer holds the process has finished it, so there is nothing left to signal.
		let request = tg::process::control::ServerRequestArg::Signal(
			tg::process::control::SignalServerRequestArg { signal: arg.signal },
		);
		let reply = control.control_sender.request(request).await?;
		match reply {
			std::ops::ControlFlow::Continue(_)
			| std::ops::ControlFlow::Break(tg::process::control::ClientResponseOutput::Signal(_)) => {
				Ok(Some(()))
			},
			std::ops::ControlFlow::Break(_) => Err(tg::error!("expected a signal response")),
		}
	}

	pub(crate) async fn try_post_process_signal_local(
		&self,
		session: &Session,
		id: &tg::process::Id,
		request_id: &str,
		signal: tg::process::Signal,
		tokens: &[tg::authorization::Token],
		exhaustive: bool,
	) -> tg::Result<Option<()>> {
		// Authorize the signal.
		let node = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		let authorized = self
			.authorize_process_get(session, id, false, false, tokens, exhaustive)
			.boxed()
			.await?;
		if !authorized.permissions.contains(node) {
			return Ok(None);
		}
		let parent = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent,
		);
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let authorized = session
			.authorize_with_exhaustive(resource, parent, exhaustive)
			.await?
			.check_exhaustion()?;
		if !authorized.permissions.contains(parent) {
			return Ok(None);
		}

		// Subscribe before sending the request, so that a finish between the request and the wait cannot be missed.
		let mut wakeups = self
			.create_process_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;

		// Send the request to the process's holder.
		let request = tg::process::control::ServerRequestArg::Signal(
			tg::process::control::SignalServerRequestArg { signal },
		);
		let retry = session.server.config.control.request_retry.clone().into();
		let timeout = if session
			.server
			.config
			.roles
			.contains(&tangram_server_core::config::Role::Runner)
		{
			session.server.config.runner.stdio_drain_timeout
		} else {
			session.server.config.control.request_timeout
		};
		let options = tangram_server_core::control::Options { retry, timeout };
		let arg = crate::holder::Arg {
			checkpoint: Some("process.signal"),
			deadline: session.server.control_read_deadline(),
			exhaustive,
			fallback: false,
			id: request_id.to_owned(),
			options,
			request,
			wakeups: Some(&mut wakeups),
		};
		let output = self
			.request_process_holder(session, id, arg, |_| false)
			.boxed()
			.await?
			.ok_or_else(|| tg::error!("the process status wakeup stream ended"))?;

		// A process in a remote is not signaled here, and a cacheable process cannot be signaled.
		let (process, data) = output.indexed.unzip();
		if process
			.as_ref()
			.and_then(|process| process.location.as_ref())
			.is_some_and(tg::Location::is_remote)
		{
			return Ok(None);
		}
		if data.flatten().is_some_and(|data| data.cacheable) {
			return Err(tg::error!(%id, "cannot signal cacheable processes"));
		}

		// A finished process has nothing left to signal, and a process that neither the index nor a holder has does not exist.
		let finished = process
			.and_then(|process| process.status)
			.is_some_and(|status| status.is_finished());
		match output.reply {
			Some(Ok(tg::process::control::ClientResponseOutput::Signal(_))) => Ok(Some(())),
			Some(Ok(_)) => Err(tg::error!("expected a signal response")),
			Some(Err(_)) | None if finished => Ok(Some(())),
			Some(Err(error)) => Err(error),
			None => Ok(None),
		}
	}

	async fn try_post_process_signal_regions(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		signal: tg::process::Signal,
		regions: &[String],
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<()>> {
		let mut futures = regions
			.iter()
			.map(|region| {
				self.try_post_process_signal_region(session, process, signal, region, tokens)
			})
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
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		signal: tg::process::Signal,
		region: &str,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<()>> {
		let client = session
			.get_region_session_for_process(region)
			.await
			.map_err(
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
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		signal: tg::process::Signal,
		remotes: &[tangram_server_core::location::Remote],
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<()>> {
		let mut futures = remotes
			.iter()
			.map(|remote| {
				self.try_post_process_signal_remote(session, process, signal, remote, tokens)
			})
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
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		signal: tg::process::Signal,
		remote: &tangram_server_core::location::Remote,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<()>> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session_for_process(session, &remote.name)
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
		session: &Session,
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
			.try_post_process_signal(session, &process, arg)
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
