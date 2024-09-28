use crate::Server;
use num::ToPrimitive;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn health(&self) -> tg::Result<tg::server::Health> {
		// Get a database connection.
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		// Get the build health.
		#[derive(serde::Deserialize)]
		struct Row {
			created: u64,
			dequeued: u64,
			started: u64,
		}
		let statement = "
			select
				(select count(*) from builds where status = 'created') as created,
				(select count(*) from builds where status = 'dequeued') as dequeued,
				(select count(*) from builds where status = 'started') as started;
		"
		.to_owned();
		let params = db::params![];
		let Row {
			created,
			dequeued,
			started,
		} = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let builds = tg::server::health::Builds {
			created,
			dequeued,
			started,
		};

		// Drop the database connection.
		drop(connection);

		let available_connections = match &self.database {
			Either::Left(database) => database.pool().available().to_u64().unwrap(),
			Either::Right(database) => database.pool().available().to_u64().unwrap(),
		};
		let database = tg::server::health::Database {
			available_connections,
		};

		let file_descriptor_semaphore = tg::server::health::FileDescriptorSemaphore {
			available_permits: self
				.file_descriptor_semaphore
				.available_permits()
				.to_u64()
				.unwrap(),
		};

		let health = tg::server::Health {
			builds: Some(builds),
			database: Some(database),
			file_descriptor_semaphore: Some(file_descriptor_semaphore),
			version: self.options.version.clone(),
		};

		Ok(health)
	}
}

impl Server {
	pub(crate) async fn handle_server_clean_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		handle.clean().await?;
		Ok(http::Response::builder().empty().unwrap())
	}

	pub(crate) async fn handle_server_health_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let health = handle.health().await?;
		let body = serde_json::to_vec(&health).unwrap();
		let response = http::Response::builder().bytes(body).unwrap();
		Ok(response)
	}
}
