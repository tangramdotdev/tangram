use crate::{options, util::task::Stop, Server};
use db::{Database, Query};
use futures::{
	future::{self, Either},
	stream::FuturesUnordered,
	StreamExt,
};
use std::pin::pin;
use tangram_client as tg;
use tangram_database as db;
use time::format_description::well_known::Rfc3339;

pub struct Monitor {
	stop: Stop,
	heartbeat_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
	enqueue_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Server {
	pub async fn try_start_build_monitor(
		&self,
		options: &options::BuildMonitor,
	) -> tg::Result<Monitor> {
		// Create the stop signal.
		let stop = Stop::new();

		// Create the heartbeat task.
		let heartbeat_task = tokio::task::spawn({
			let server = self.clone();
			let stop = stop.clone();
			let timeout = options.heartbeat_timeout;
			let interval = options.interval;
			let limit = options.heartbeat_limit;
			async move {
				loop {
					let stop = stop.stopped();
					let tick = tokio::time::sleep(interval);

					// Wait for either the stop signal or an interval.
					if let Either::Left(_) = future::select(pin!(stop), pin!(tick)).await {
						break;
					}

					// Reap stale builds if the timeout is set.
					server
						.try_reap_builds(timeout, limit)
						.await
						.inspect_err(|error| tracing::error!(%error, "failed to reap builds"))
						.ok();
				}
			}
		});

		// Create the enqueue task.
		let enqueue_task = tokio::task::spawn({
			let server = self.clone();
			let stop = stop.clone();
			let interval = options.interval;
			let timeout = options.dequeue_timeout;
			async move {
				loop {
					let stop = stop.stopped();
					let tick = tokio::time::sleep(interval);

					// Wait for either the stop signal or an interval.
					if let Either::Left(_) = future::select(pin!(stop), pin!(tick)).await {
						break;
					}

					// Reenqueue builds that were dequeued but not star
					server
						.try_reenqueue_builds(timeout)
						.await
						.inspect_err(|error| tracing::error!(%error, "failed to reap builds"))
						.ok();
				}
			}
		});

		let heartbeat_task = std::sync::Mutex::new(Some(heartbeat_task));
		let enqueue_task = std::sync::Mutex::new(Some(enqueue_task));
		Ok(Monitor {
			stop,
			heartbeat_task,
			enqueue_task,
		})
	}

	async fn try_reap_builds(&self, timeout: std::time::Duration, limit: u32) -> tg::Result<()> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get all builds with heartbeat_at older than now - timeout.
		loop {
			#[derive(serde::Deserialize)]
			struct Row {
				id: tg::build::Id,
			}
			let p = connection.p();
			let statement = format!(
				"
					select id from builds
					where
						heartbeat_at <= {p}1 and
						status = 'started'
					limit {p}2;
				"
			);
			let time = time::OffsetDateTime::now_utc() - timeout;
			let params = db::params![time.format(&Rfc3339).unwrap(), limit];
			let builds = connection
				.query_all_into::<Row>(statement, params)
				.await
				.inspect_err(|error| tracing::error!(%error, "failed to perform query"))
				.map_err(|source| tg::error!(!source, "failed to perform the query"))?;

			if builds.is_empty() {
				break;
			}

			// Cancel builds that have missed a heartbeat.
			builds
				.into_iter()
				.map(|row| async move {
					let id = row.id;
					let build = tg::Build::with_id(id.clone());
					build
						.cancel(self)
						.await
						.inspect_err(|error| tracing::error!(%error, %id, "failed to cancel build"))
						.ok()
				})
				.collect::<FuturesUnordered<_>>()
				.collect::<Vec<_>>()
				.await;
		}

		Ok(())
	}

	async fn try_reenqueue_builds(&self, timeout: std::time::Duration) -> tg::Result<()> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get all builds whose dequeued_at is older than now - timeout and update their status to "created"
		let p = connection.p();
		let statement = format!(
			"
				update builds
				set status = 'created'
				where
					dequeued_at <= {p}1 and
					status = 'dequeued';
			"
		);
		let time = time::OffsetDateTime::now_utc() - timeout;
		let params = db::params!(time.format(&Rfc3339).unwrap());
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}
}

impl Monitor {
	pub fn stop(&self) {
		self.stop.stop();
	}

	pub async fn wait(&self) {
		let heartbeat_task = self.heartbeat_task.lock().unwrap().take();
		if let Some(heartbeat_task) = heartbeat_task {
			heartbeat_task.await.ok();
		}
		let enqueue_task = self.enqueue_task.lock().unwrap().take();
		if let Some(enqueue_task) = enqueue_task {
			enqueue_task.await.ok();
		}
	}
}
