use {
	crate::Component,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::BoxFuture,
		stream::{self, FuturesUnordered},
	},
	tangram_client::prelude::*,
	tangram_futures::stream::Ext as _,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
};

type WaitFuture = BoxFuture<'static, tg::Result<Option<tg::sandbox::outcome::Data>>>;

impl Component {
	pub async fn try_wait_sandbox_future(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::wait::Arg,
	) -> tg::Result<Option<WaitFuture>> {
		if !arg.source.is_index()
			&& let Some(future) = self.try_wait_sandbox_runner(session, id, &arg).await?
		{
			return Ok(Some(future));
		}
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(future) = self
					.try_wait_sandbox_local(
						session,
						id,
						arg.tokens.local_authorization(),
						arg.source,
					)
					.await?
			{
				return Ok(Some(future));
			}

			if let Some(future) = self
				.try_wait_sandbox_regions(session, id, &local.regions, &arg.tokens, arg.source)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to wait for the sandbox in another region"),
				)? {
				return Ok(Some(future));
			}
		}

		if let Some(future) = self
			.try_wait_sandbox_remotes(session, id, &locations.remotes, &arg.tokens, arg.source)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to wait for the sandbox on a remote"),
			)? {
			return Ok(Some(future));
		}

		Ok(None)
	}

	async fn try_wait_sandbox_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::wait::Arg,
	) -> tg::Result<Option<WaitFuture>> {
		let Some(mut runner) = session
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
				tg::authorization::permission::sandbox::Permission::Node,
			)
			.await?
		{
			return Ok(None);
		}
		let session = session.clone();
		let id = id.clone();
		let mut arg = arg.clone();
		let future = async move {
			loop {
				let data = session
					.server
					.components
					.runner
					.state()
					.sandboxes()
					.get(runner.index)
					.map(|sandbox| sandbox.data().data);
				let Some(data) = data else {
					// Resume at the owning location when the runner releases its state.
					arg.location = Some(runner.location_arg);
					let future = session
						.server
						.components
						.sandbox
						.try_wait_sandbox_future(&session, &id, arg)
						.await?
						.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
					return future.await;
				};
				if data.status.is_destroyed() {
					let outcome = Self::create_sandbox_outcome_data(&data);
					return Ok(Some(outcome));
				}
				runner.changed.changed().await.ok();
			}
		};
		Ok(Some(future.boxed()))
	}

	async fn try_wait_sandbox_local(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		tokens: &[tg::authorization::Token],
		source: tg::sandbox::Source,
	) -> tg::Result<Option<WaitFuture>> {
		let mut wakeups = self
			.create_sandbox_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;
		let deadline = session.server.control_read_deadline();
		let sandbox = loop {
			tokio::select! {
				output = self.try_get_sandbox_observation_local(session, id, tokens, source, deadline) => break output?,
				wakeup = wakeups.next() => {
					if wakeup.is_none() { return Ok(None); }
				},
			}
		};
		let Some(sandbox) = sandbox else {
			return Ok(None);
		};

		let mut stream = self.create_sandbox_data_stream_local(
			session,
			id,
			Some(sandbox),
			Some(wakeups),
			source,
		);
		let future = async move {
			let data = loop {
				let data = stream.try_next().await?.ok_or_else(|| {
					tg::error!("the sandbox status stream ended before the sandbox was destroyed")
				})?;
				if data.status.is_destroyed() {
					break data;
				}
			};
			let outcome = Self::create_sandbox_outcome_data(&data);
			Ok(Some(outcome))
		};

		Ok(Some(future.boxed()))
	}

	async fn try_wait_sandbox_regions(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		regions: &[String],
		tokens: &tg::authorization::Tokens,
		source: tg::sandbox::Source,
	) -> tg::Result<Option<WaitFuture>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_wait_sandbox_region(session, id, region, tokens, source))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(future)) => {
					result = Ok(Some(future));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(future) = result? else {
			return Ok(None);
		};
		Ok(Some(future))
	}

	async fn try_wait_sandbox_region(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		region: &str,
		tokens: &tg::authorization::Tokens,
		source: tg::sandbox::Source,
	) -> tg::Result<Option<WaitFuture>> {
		let client = session.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::wait::Arg {
			location: Some(location.clone().into()),
			source,
			tokens: tokens.for_location(&location),
		};
		let Some(future) = client.try_wait_sandbox_future(id, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to wait for the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(future.boxed()))
	}

	async fn try_wait_sandbox_remotes(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		remotes: &[tangram_server_core::location::Remote],
		tokens: &tg::authorization::Tokens,
		source: tg::sandbox::Source,
	) -> tg::Result<Option<WaitFuture>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_wait_sandbox_remote(session, id, remote, tokens, source))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(future)) => {
					result = Ok(Some(future));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(future) = result? else {
			return Ok(None);
		};
		Ok(Some(future))
	}

	async fn try_wait_sandbox_remote(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		remote: &tangram_server_core::location::Remote,
		tokens: &tg::authorization::Tokens,
		source: tg::sandbox::Source,
	) -> tg::Result<Option<WaitFuture>> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let arg = tg::sandbox::wait::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			source,
			tokens: tokens.for_location(&location),
		};
		let Some(future) = client.try_wait_sandbox_future(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to wait for the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(future.boxed()))
	}

	fn create_sandbox_outcome_data(data: &tg::sandbox::Data) -> tg::sandbox::outcome::Data {
		tg::sandbox::outcome::Data {
			error: data.error.clone(),
		}
	}

	pub(crate) async fn try_wait_sandbox_future_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the ID.
		let id = id
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the sandbox id"))?;

		// Parse the arg.
		let (arg, request) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the future.
		let Some(future) = self.try_wait_sandbox_future(session, &id, arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the stream.
		let stream = stream::once(future)
			.filter_map(|result| async move {
				match result {
					Ok(Some(outcome)) => Some(Ok(tg::sandbox::wait::Event::Outcome(outcome))),
					Ok(None) => None,
					Err(error) => Some(Err(error)),
				}
			})
			.with_stopper(session.context.stopper.clone());

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), BoxBody::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
