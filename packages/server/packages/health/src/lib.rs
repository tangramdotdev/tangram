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

	pub(crate) async fn health(
		&self,
		session: &Session,
		arg: tg::health::Arg,
	) -> tg::Result<tg::Health> {
		let fields = arg.fields.as_ref();
		if let Some(fields) = fields {
			for field in fields {
				match field.as_str() {
					"database" | "diagnostics" | "processes" | "version" => (),
					_ => return Err(tg::error!(%field, "invalid health field")),
				}
			}
		}
		if session
			.server
			.try_get_request_origin_sandbox(session.context.origin)?
			.is_some()
			&& fields.is_none_or(|fields| {
				fields
					.iter()
					.any(|field| !matches!(field.as_str(), "diagnostics" | "version"))
			}) {
			return Err(tg::error!("unauthorized"));
		}

		let include_field = |name: &str| match fields {
			Some(fields) => fields.iter().any(|field| field == name),
			None => true,
		};
		let include_database = include_field("database");
		let include_diagnostics = include_field("diagnostics");
		let include_processes = include_field("processes");
		let include_version = include_field("version");

		let processes = if include_processes {
			// Get the process health.
			let capacity = if session
				.server
				.config
				.roles
				.contains(&tangram_server_core::config::Role::Runner)
			{
				Some(session.server.components.runner.state().capacity().get())
			} else {
				None
			};
			let started = session
				.server
				.components
				.runner
				.state()
				.started_process_count();

			Some(tg::health::Processes { capacity, started })
		} else {
			None
		};

		let database = include_database.then(|| session.server.database.health());

		let health = tg::Health {
			database,
			diagnostics: include_diagnostics.then(|| {
				session
					.server
					.diagnostics
					.lock()
					.unwrap()
					.iter()
					.map(tg::Diagnostic::to_data)
					.collect()
			}),
			processes,
			version: include_version.then(|| session.server.version.clone()),
		};

		Ok(health)
	}

	pub(crate) async fn health_request(
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
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Get the health.
		let health = self
			.health(session, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the server health"))?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&health).unwrap();
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
			(http::Method::GET, ["health"]) => self.health_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::health::Component for Component {
	fn health<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::health::Arg,
	) -> BoxFuture<'a, tg::Result<tg::Health>> {
		self.health(session, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
