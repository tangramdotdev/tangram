use {
	crate::{Session, database::Database},
	num::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Session {
	pub(crate) async fn health(&self, arg: tg::health::Arg) -> tg::Result<tg::Health> {
		let fields = arg.fields.as_ref();
		if let Some(fields) = fields {
			for field in fields {
				match field.as_str() {
					"database" | "diagnostics" | "processes" | "version" => (),
					_ => return Err(tg::error!(%field, "invalid health field")),
				}
			}
		}

		if matches!(self.context.principal, tg::Principal::Process(_))
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
			let capacity = if self.server.config.runner.is_some() {
				Some(self.server.runner.state.capacity.get())
			} else {
				None
			};
			let started = self.server.runner.state.started_process_count();

			Some(tg::health::Processes { capacity, started })
		} else {
			None
		};

		let database = if include_database {
			let available_connections = match &self.server.database {
				#[cfg(feature = "postgres")]
				Database::Postgres(database) => database.pool().available().to_u64().unwrap(),
				#[cfg(feature = "sqlite")]
				Database::Sqlite(database) => {
					database.read_pool().available().to_u64().unwrap()
						+ database.write_pool().available().to_u64().unwrap()
				},
				#[cfg(feature = "turso")]
				Database::Turso(database) => database.pool().available().to_u64().unwrap(),
			};

			Some(tg::health::Database {
				available_connections,
			})
		} else {
			None
		};

		let health = tg::Health {
			database,
			diagnostics: include_diagnostics.then(|| {
				self.server
					.diagnostics
					.lock()
					.unwrap()
					.iter()
					.map(tg::Diagnostic::to_data)
					.collect()
			}),
			processes,
			version: include_version.then(|| self.server.version.clone()),
		};

		Ok(health)
	}

	pub(crate) async fn health_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the health.
		let health = self
			.health(arg)
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
