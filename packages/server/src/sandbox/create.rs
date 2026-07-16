use {
	crate::{Server, Session},
	futures::{TryStreamExt as _, future},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::Messenger as _,
};

mod arg;

impl Session {
	pub(crate) async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
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
			.or_else(|| Some(creator.clone()))
			.filter(|owner| !matches!(owner, tg::Principal::Root));
		arg.owner.clone_from(&owner);
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

		let mut connected = self
			.server
			.messenger
			.subscribe::<()>(super::control::connected_subject(&id))
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to subscribe to the sandbox control connection"
				)
			})?;

		let create_future = async {
			let request = crate::scheduler::CreateSandboxRequestArg {
				arg,
				creator: Some(creator),
				parent: None,
				process: None,
				sandbox: id.clone(),
				token: Some(token),
			};
			self.enqueue_sandbox(request)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to enqueue the sandbox"))?;
			Ok::<_, tg::Error>(())
		};
		let connect_future = async {
			connected
				.try_next()
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to receive the sandbox control connection")
				})?
				.ok_or_else(|| tg::error!("the sandbox control connection stream ended"))?;
			Ok::<_, tg::Error>(())
		};
		future::try_join(create_future, connect_future).await?;

		let output = tg::sandbox::create::Output { id };

		Ok(output)
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
