use {
	futures::{FutureExt as _, future::BoxFuture},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::Session,
};

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn check(
		&self,
		session: &Session,
		arg: tg::check::Arg,
	) -> tg::Result<tg::check::Output> {
		let location = session.server.location(arg.location.as_ref())?;

		let output = match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				self.check_region(session, arg, region).await?
			},
			tg::Location::Local(_) => self.check_local(session, arg).await?,
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => self.check_remote(session, arg, remote, region).await?,
		};

		Ok(output)
	}

	async fn check_local(
		&self,
		session: &Session,
		arg: tg::check::Arg,
	) -> tg::Result<tg::check::Output> {
		if !session.server.checkouts_enabled() {
			return Err(tg::error!("checkouts are disabled"));
		}
		// Create the compiler.
		let compiler = session.create_compiler();

		// Check the modules.
		let diagnostics = compiler
			.check(arg.modules.clone())
			.await
			.map_err(|error| tg::error!(!error, "failed to check the modules"))?;

		// Get the modules with check exports if the modules check without errors, with the interfaces that checking them prepared.
		let checks = if diagnostics
			.iter()
			.any(|diagnostic| diagnostic.severity.is_error())
		{
			Vec::new()
		} else {
			compiler
				.check_exports(&arg.modules)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the check exports"))?
		};

		// Create the output.
		let diagnostics = diagnostics.iter().map(tg::Diagnostic::to_data).collect();
		let output = tg::check::Output {
			checks,
			diagnostics,
		};

		// Stop and await the compiler.
		compiler.stop();
		compiler
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "failed to wait for the compiler"))?;

		Ok(output)
	}

	async fn check_region(
		&self,
		session: &Session,
		arg: tg::check::Arg,
		region: String,
	) -> tg::Result<tg::check::Output> {
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::check::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client
			.check(arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to check"))?;
		Ok(output)
	}

	async fn check_remote(
		&self,
		session: &Session,
		arg: tg::check::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<tg::check::Output> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
			)?;
		let arg = tg::check::Arg {
			location: Some(region.as_deref().map_or_else(
				|| tg::Location::Local(tg::location::Local::default()).into(),
				|region| {
					tg::Location::Local(tg::location::Local {
						region: Some(region.to_owned()),
					})
					.into()
				},
			)),
			..arg
		};
		let output = client
			.check(arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to check"))?;
		Ok(output)
	}

	pub(crate) async fn check_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Check the modules.
		let output = self
			.check(session, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to check the modules"))?;

		// Create the response.
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

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["check"]) => self.check_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::check::Component for Component {
	fn check<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::check::Arg,
	) -> BoxFuture<'a, tg::Result<tg::check::Output>> {
		self.check(session, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
