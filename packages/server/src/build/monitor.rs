use crate::{options, Server};
use futures::future;
use futures::{stream::FuturesUnordered, StreamExt as _};
use num::ToPrimitive;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*, Query};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn build_monitor_task(&self, options: &options::BuildMonitor) {
		// Create the dequeue future.
		let server = self.clone();
		let interval = options.dequeue_interval;
		let limit = options.dequeue_limit;
		let timeout = options.dequeue_timeout;
		let dequeue = async move {
			loop {
				let result = server
					.build_monitor_dequeue_task_inner(timeout, limit)
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to reenqueue builds"));
				if matches!(result, Err(_) | Ok(0)) {
					tokio::time::sleep(interval).await;
				}
			}
		};

		// Create the heartbeat future.
		let server = self.clone();
		let interval = options.heartbeat_interval;
		let limit = options.heartbeat_limit;
		let timeout = options.heartbeat_timeout;
		let heartbeat = async move {
			loop {
				let result = server
					.build_monitor_heartbeat_task_inner(timeout, limit)
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to cancel builds"));
				if matches!(result, Err(_) | Ok(0)) {
					tokio::time::sleep(interval).await;
				}
			}
		};

		future::join(heartbeat, dequeue).await;
	}

	async fn build_monitor_dequeue_task_inner(
		&self,
		timeout: std::time::Duration,
		limit: u64,
	) -> tg::Result<u64> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Update all dequeued builds whose dequeued_at exceeds the timeout to be created.
		let p = connection.p();
		let statement = format!(
			"
				update builds
				set status = 'created'
				where
					status = 'dequeued' and
					dequeued_at <= {p}1
				limit {p}2
				returning id;
			"
		);
		let time = (time::OffsetDateTime::now_utc() - timeout)
			.format(&Rfc3339)
			.unwrap();
		let params = db::params![time, limit];
		let builds = connection
			.query_all_value_into::<tg::build::Id>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(builds.len().to_u64().unwrap())
	}

	pub(crate) async fn build_monitor_heartbeat_task_inner(
		&self,
		timeout: std::time::Duration,
		limit: u64,
	) -> tg::Result<u64> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get all started builds whose heartbeat_at exceeds the timeout.
		let p = connection.p();
		let statement = format!(
			"
				select id
				from builds
				where
					status = 'started' and
					heartbeat_at <= {p}1
				limit {p}2;
			"
		);
		let time = (time::OffsetDateTime::now_utc() - timeout)
			.format(&Rfc3339)
			.unwrap();
		let params = db::params![time, limit];
		let builds = connection
			.query_all_value_into::<tg::build::Id>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Cancel the builds.
		builds
			.iter()
			.map(|id| async move {
				let build = tg::Build::with_id(id.clone());
				let arg = tg::build::finish::Arg {
					outcome: tg::build::outcome::Data::Canceled,
					remote: None,
				};
				build
					.finish(self, arg)
					.await
					.inspect_err(|error| tracing::error!(%error, %id, "failed to cancel the build"))
					.ok()
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		Ok(builds.len().to_u64().unwrap())
	}
}
