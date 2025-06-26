use crate::Server;
use num::ToPrimitive as _;
use std::time::Duration;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{Body, response::builder::Ext as _};

impl Server {
	pub async fn health(&self) -> tg::Result<tg::Health> {
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
		#[derive(serde::Deserialize)]
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
		let processes = tg::health::Processes {
			created,
			enqueued,
			dequeued,
			permits,
			started,
		};

		// Drop the database connection.
		drop(connection);

		let available_connections = match &self.database {
			Either::Left(database) => {
				database.read_pool().available().to_u64().unwrap()
					+ database.write_pool().available().to_u64().unwrap()
			},
			Either::Right(database) => database.pool().available().to_u64().unwrap(),
		};
		let database = tg::health::Database {
			available_connections,
		};

		let health = tg::Health {
			database: Some(database),
			diagnostics: self.diagnostics.lock().unwrap().clone(),
			processes: Some(processes),
			version: Some(self.version.clone()),
		};

		Ok(health)
	}

	pub(crate) async fn diagnostics_task(&self) -> tg::Result<()> {
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

	pub(crate) async fn handle_server_health_request<H>(
		handle: &H,
		_request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let health = handle.health().await?;
		let body = serde_json::to_vec(&health).unwrap();
		let response = http::Response::builder().bytes(body).unwrap();
		Ok(response)
	}
}
