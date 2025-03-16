use crate::Server;
use num::ToPrimitive;
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
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		// Get the process health.
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

		let file_descriptor_semaphore = tg::health::FileDescriptorSemaphore {
			available_permits: self
				.file_descriptor_semaphore
				.available_permits()
				.to_u64()
				.unwrap(),
		};

		let health = tg::Health {
			processes: Some(processes),
			database: Some(database),
			file_descriptor_semaphore: Some(file_descriptor_semaphore),
			version: Some(self.version.clone()),
		};

		Ok(health)
	}
}

impl Server {
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
