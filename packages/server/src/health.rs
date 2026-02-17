use {
	crate::{Context, Server, database::Database},
	num::ToPrimitive as _,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Server {
	pub(crate) async fn health_with_context(
		&self,
		context: &Context,
		arg: tg::health::Arg,
	) -> tg::Result<tg::Health> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let fields = arg.fields.as_ref();
		if let Some(fields) = fields {
			for field in fields {
				match field.as_str() {
					"database" | "diagnostics" | "pipes" | "processes" | "ptys" | "version" => (),
					_ => return Err(tg::error!(%field, "invalid health field")),
				}
			}
		}

		let include_field = |name: &str| match fields {
			Some(fields) => fields.iter().any(|field| field == name),
			None => true,
		};
		let include_database = include_field("database");
		let include_diagnostics = include_field("diagnostics");
		let include_pipes = include_field("pipes");
		let include_processes = include_field("processes");
		let include_ptys = include_field("ptys");
		let include_version = include_field("version");

		let processes = if include_processes {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Get the process health.
			let permits = if self.config.runner.is_some() {
				Some(self.process_semaphore.available_permits().to_u64().unwrap())
			} else {
				None
			};
			#[derive(db::row::Deserialize)]
			struct Row {
				created: u64,
				enqueued: u64,
				dequeued: u64,
				started: u64,
			}
			let statement = "
				select
					(select count(*) from processes where status = 'created') as created,
					(select count(*) from processes where status = 'enqueued') as enqueued,
					(select count(*) from processes where status = 'dequeued') as dequeued,
					(select count(*) from processes where status = 'started') as started;
			"
			.to_owned();
			let params = db::params![];
			let Row {
				created,
				enqueued,
				dequeued,
				started,
			} = connection
				.query_one_into::<Row>(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			Some(tg::health::Processes {
				created,
				enqueued,
				dequeued,
				permits,
				started,
			})
		} else {
			None
		};

		let database = if include_database {
			let available_connections = match &self.database {
				#[cfg(feature = "postgres")]
				Database::Postgres(database) => database.pool().available().to_u64().unwrap(),
				#[cfg(feature = "sqlite")]
				Database::Sqlite(database) => {
					database.read_pool().available().to_u64().unwrap()
						+ database.write_pool().available().to_u64().unwrap()
				},
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
				self.diagnostics
					.lock()
					.unwrap()
					.iter()
					.map(tg::Diagnostic::to_data)
					.collect()
			}),
			pipes: include_pipes
				.then(|| self.pipes.iter().map(|entry| entry.key().clone()).collect()),
			processes,
			ptys: include_ptys.then(|| self.ptys.iter().map(|entry| entry.key().clone()).collect()),
			version: include_version.then(|| self.version.clone()),
		};

		Ok(health)
	}

	pub(crate) async fn diagnostics_task(&self) -> tg::Result<()> {
		if self.config.advanced.disable_version_check {
			return Ok(());
		}
		loop {
			let mut diagnostics = Vec::new();
			if let Some(latest) = self.try_get_latest_version().await {
				let version = &self.version;
				if &latest != version {
					diagnostics.push(tg::Diagnostic {
						location: None,
						severity: tg::diagnostic::Severity::Warning,
						message: format!(
							r#"A new version of tangram is available. The latest version is "{latest}". You are on version "{version}"."#,
						),
					});
				}
			}
			*self.diagnostics.lock().unwrap() = diagnostics;
			tokio::time::sleep(Duration::from_secs(3600)).await;
		}
	}

	async fn try_get_latest_version(&self) -> Option<String> {
		#[derive(serde::Deserialize)]
		struct Output {
			name: String,
		}
		let output: Output = reqwest::Client::new()
			.request(
				http::Method::GET,
				"https://api.github.com/repos/tangramdotdev/tangram/releases/latest",
			)
			.header("Accept", "application/vnd.github+json")
			.header("User-Agent", "tangram")
			.send()
			.await
			.inspect_err(|error| tracing::warn!(%error, "failed to get response from github"))
			.ok()?
			.json()
			.await
			.inspect_err(
				|error| tracing::warn!(%error, "failed to deserialize response from github"),
			)
			.ok()?;
		Some(output.name)
	}

	pub(crate) async fn handle_server_health_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the health.
		let health = self
			.health_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the server health"))?;

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
