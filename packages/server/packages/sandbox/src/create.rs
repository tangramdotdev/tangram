use {
	crate::Component,
	futures::FutureExt as _,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
};

mod arg;

impl Component {
	pub(crate) async fn try_create_sandbox(
		&self,
		session: &Session,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<Option<tg::sandbox::create::Output>> {
		let parent = session
			.server
			.try_get_request_origin_sandbox(session.context.origin)?
			.map(|sandbox| sandbox.id.clone());
		if let Some(parent) = parent {
			self.validate_sandbox_create_arg_with_parent(session, &arg, &parent)
				.await?;
		}

		// Determine whether the create tries the other locations, one at a time, when a location declines it, which a create with an explicit location does not.
		let walk = arg.location.is_none();

		// Create the sandbox at the first location, and then at each other location until one accepts it.
		let mut location = session.server.location(arg.location.as_ref())?;
		let mut tried = Vec::new();
		let mut candidates = None;
		let output = loop {
			let output = match location.clone() {
				tg::Location::Local(tg::location::Local {
					region: Some(region),
				}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
					self.try_create_sandbox_region(session, arg.clone(), region)
						.await?
				},
				tg::Location::Local(_) => {
					self.try_create_sandbox_local(session, arg.clone()).await?
				},
				tg::Location::Remote(tg::location::Remote {
					name: remote,
					region,
				}) => {
					self.try_create_sandbox_remote(session, arg.clone(), remote, region)
						.await?
				},
			};
			if output.is_some() || !walk {
				break output;
			}
			tried.push(session.server.normalize_location(location));
			if candidates.is_none() {
				candidates = Some(session.candidate_locations().await?);
			}
			let next = candidates
				.as_ref()
				.unwrap()
				.iter()
				.find(|candidate| !tried.contains(candidate))
				.cloned();
			let Some(next) = next else {
				break None;
			};
			location = next;
		};

		Ok(output)
	}

	#[tracing::instrument(name = "sandbox.schedule", level = "info", skip_all, fields(sandbox = tracing::field::Empty), err(level = "debug"))]
	async fn try_create_sandbox_local(
		&self,
		session: &Session,
		mut arg: tg::sandbox::create::Arg,
	) -> tg::Result<Option<tg::sandbox::create::Output>> {
		// Authorize and normalize the sandbox request.
		if matches!(session.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		session
			.server
			.components
			.authorization
			.authorize_owner(session, arg.owner.as_ref())
			.await?;

		arg = tangram_server_sandbox_common::normalize_sandbox_create_arg(arg)?;
		if arg.host.is_none() {
			return Err(tg::error!("missing sandbox host"));
		}

		// Resolve the sandbox owner.
		let creator = session.context.principal.clone();
		let owner = arg.owner.clone().unwrap_or_else(|| creator.clone());
		session
			.server
			.components
			.billing
			.verify_billing(session, &owner)
			.await?;

		// Validate the backend and resources.
		tangram_server_sandbox_common::clear_darwin_sandbox_resources(&mut arg);
		let isolation = session
			.server
			.components
			.runner
			.resolve_sandbox_backend(&session.server)?;
		tangram_server_sandbox_common::validate_sandbox_resources(
			&isolation,
			arg.cpu,
			arg.memory,
			arg.hostname.as_deref(),
			arg.storage,
		)?;

		// Create the sandbox identity and control token.
		let id = tg::sandbox::Id::new();
		tracing::Span::current().record("sandbox", tracing::field::display(&id));
		tracing::info!(sandbox = %id, "scheduling the sandbox");
		let token = session
			.server
			.components
			.authentication
			.create_sandbox_authentication_token(&session.server, id.clone())?;

		// Subscribe before enqueuing the sandbox, so that a wait cannot miss a status change.
		let wait = arg.wait;
		let mut wakeups = if wait {
			let wakeups = self
				.create_sandbox_status_wakeup_stream(
					session,
					&id,
					session.context.stopper.clone(),
					None,
				)
				.await?;
			Some(wakeups)
		} else {
			None
		};

		// Enqueue the sandbox, which the scheduler accepts once its record is durable.
		let owner_ancestors = session
			.server
			.components
			.scheduler
			.get_owner_ancestors(session, &owner)
			.await?;
		let request = tangram_server_scheduler_core::EnqueueSandboxRequestArg {
			arg,
			boot: 0,
			created_at: session.server.clock.unix_timestamp(),
			creator,
			owner,
			owner_ancestors,
			parent: None,
			process: None,
			public: false,
			runner: None,
			sandbox: id.clone(),
			scheduler: None,
			token: Some(token),
		};
		let data =
			tangram_server_scheduler_core::sandbox_data(&request, tg::sandbox::Status::Starting);
		let scheduler = session
			.server
			.components
			.scheduler
			.try_enqueue_sandbox(session, request)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to enqueue the sandbox"))?;
		if scheduler.is_none() {
			return Ok(None);
		}

		// Wait for the sandbox to start if requested.
		if let Some(wakeups) = &mut wakeups {
			self.wait_for_sandbox_boot(session, &id, 0, wakeups).await?;
			let arg = tg::sandbox::get::Arg {
				location: Some(tg::Location::Local(tg::location::Local::default()).into()),
				..Default::default()
			};
			let output = self
				.try_get_sandbox(session, &id, arg)
				.boxed()
				.await?
				.ok_or_else(|| tg::error!(%id, "failed to get the created sandbox"))?;
			let output = tg::sandbox::create::Output {
				data: output.data,
				location: output.location,
				tokens: output.tokens,
			};
			return Ok(Some(output));
		}

		// Create the output from the request.
		let mut tokens = tg::authorization::Tokens::default();
		if let Some(token) = session
			.server
			.components
			.authorization
			.create_read_token(session, &id.clone().into())?
		{
			tokens.insert_local_authorization(token);
		}
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let output = tg::sandbox::create::Output {
			data,
			location: Some(location),
			tokens,
		};

		Ok(Some(output))
	}

	async fn try_create_sandbox_region(
		&self,
		session: &Session,
		arg: tg::sandbox::create::Arg,
		region: String,
	) -> tg::Result<Option<tg::sandbox::create::Output>> {
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::sandbox::create::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client.try_create_sandbox(arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to create the sandbox"),
		)?;
		Ok(output)
	}

	async fn try_create_sandbox_remote(
		&self,
		session: &Session,
		arg: tg::sandbox::create::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<Option<tg::sandbox::create::Output>> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
			)?;
		let trusted = client.trusted();
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.clone(),
			region: region.clone(),
		});
		let arg = tg::sandbox::create::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let output = client.try_create_sandbox(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to create the sandbox"),
		)?;
		let Some(mut output) = output else {
			return Ok(None);
		};
		session
			.server
			.components
			.authorization
			.update_tokens_and_location(
				session,
				&mut output.tokens,
				Some(&mut output.location),
				&location,
				trusted,
			)?;

		Ok(Some(output))
	}

	pub(crate) async fn try_create_sandbox_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		let Some(output) = self.try_create_sandbox(session, arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
