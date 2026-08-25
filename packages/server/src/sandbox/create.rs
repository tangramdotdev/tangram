use {
	crate::{Server, Session},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

mod arg;

impl Session {
	pub(crate) async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		let parent = self
			.server
			.try_get_request_origin_sandbox(self.context.origin)?
			.and_then(|sandbox| sandbox.id.clone());
		if let Some(parent) = parent {
			self.validate_sandbox_create_arg_with_parent(&arg, &parent)
				.await?;
		}

		let location = self.server.location(arg.location.as_ref())?;

		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.create_sandbox_local(arg).await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => self.create_sandbox_region(arg, region).await?,
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => self.create_sandbox_remote(arg, remote, region).await?,
		};

		Ok(output)
	}

	async fn create_sandbox_local(
		&self,
		mut arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		self.authorize_owner(arg.owner.as_ref()).await?;

		arg = Self::normalize_sandbox_create_arg(arg)?;
		if arg.host.is_none() {
			return Err(tg::error!("missing sandbox host"));
		}
		let creator = self.context.principal.clone();
		let owner = arg
			.owner
			.clone()
			.or_else(|| (!matches!(creator, tg::Principal::Root)).then(|| creator.clone()));
		arg.owner.clone_from(&owner);
		self.verify_billing(owner.as_ref()).await?;
		let isolation = self.server.resolve_sandbox_isolation()?;
		Server::validate_sandbox_resources(
			&isolation,
			arg.cpu,
			arg.memory,
			arg.hostname.as_deref(),
		)?;

		let id = tg::sandbox::Id::new();
		let token = self
			.server
			.create_sandbox_authentication_token(id.clone())?;

		let mut connection_future = self.subscribe_sandbox_connection(&id).await?;

		let request = crate::scheduler::EnqueueSandboxRequestArg {
			arg,
			creator: Some(creator),
			parent: None,
			process: None,
			sandbox: id.clone(),
			scheduler: None,
			token: Some(token),
		};
		let scheduler = self
			.enqueue_sandbox(request)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to enqueue the sandbox"))?;
		let connected = self
			.try_wait_sandbox_connection(&scheduler, &mut connection_future)
			.await?;
		if !connected {
			self.spawn_create_sandbox_cleanup_after_scheduler_heartbeat_expiration(
				id.clone(),
				scheduler.clone(),
				connection_future,
			);
			return Err(tg::error!(
				code = tg::error::Code::HeartbeatExpiration,
				sandbox = %id,
				%scheduler,
				"the scheduler heartbeat expired"
			));
		}

		let output = tg::sandbox::create::Output { id };

		Ok(output)
	}

	async fn try_wait_sandbox_connection(
		&self,
		scheduler: &tg::scheduler::Id,
		connection_future: &mut super::ConnectionFuture,
	) -> tg::Result<bool> {
		tokio::select! {
			result = connection_future.as_mut() => result.map(|()| true),
			result = self.scheduler_heartbeat_expired(scheduler) => result.map(|()| false),
		}
	}

	fn spawn_create_sandbox_cleanup_after_scheduler_heartbeat_expiration(
		&self,
		sandbox: tg::sandbox::Id,
		scheduler: tg::scheduler::Id,
		connection_future: super::ConnectionFuture,
	) {
		let error = tg::error::Data {
			code: Some(tg::error::Code::HeartbeatExpiration),
			message: Some("the scheduler heartbeat expired".into()),
			..Default::default()
		};
		let arg = tg::sandbox::destroy::Arg {
			error: Some(tg::Either::Left(error)),
			location: Some(tg::Location::Local(tg::location::Local::default()).into()),
		};
		let session = self.server.session(&self.server.context);
		tokio::spawn(async move {
			session
				.destroy_sandbox_when_available(&sandbox, arg, connection_future)
				.await
				.inspect_err(|error| {
					tracing::error!(
						error = %error.trace(),
						%sandbox,
						%scheduler,
						"failed to destroy the sandbox after the scheduler heartbeat expired"
					);
				})
				.ok();
		});
	}

	async fn create_sandbox_region(
		&self,
		arg: tg::sandbox::create::Arg,
		region: String,
	) -> tg::Result<tg::sandbox::create::Output> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::sandbox::create::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client.create_sandbox(arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to create the sandbox"),
		)?;
		Ok(output)
	}

	async fn create_sandbox_remote(
		&self,
		arg: tg::sandbox::create::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<tg::sandbox::create::Output> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::create::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let output = client.create_sandbox(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to create the sandbox"),
		)?;
		Ok(output)
	}

	pub(crate) async fn create_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		let output = self.create_sandbox(arg).await?;

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
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
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
