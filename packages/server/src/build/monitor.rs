use crate::{options, Server};
use db::{Database, Query};
use futures::{
	future::{self, Either},
	stream::FuturesUnordered,
	TryStreamExt,
};
use std::pin::pin;
use tangram_client as tg;
use tangram_database as db;
use time::format_description::well_known::Rfc3339;

pub struct Monitor {
	stop: tokio::sync::watch::Sender<bool>,
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Server {
	pub async fn try_start_build_monitor(
		&self,
		options: &options::BuildMonitor,
	) -> tg::Result<Monitor> {
		let (stop, _) = tokio::sync::watch::channel(false);
		let task = tokio::task::spawn({
			let server = self.clone();
			let mut stop = stop.subscribe();
			let interval = options.interval;
			let heartbeat_timeout = options.heartbeat_timeout;
			let dequeue_timeout = options.dequeue_timeout;
			async move {
				let mut interval = tokio::time::interval(interval);
				loop {
					let stop = stop.wait_for(|s| *s);
					let tick = interval.tick();
					if let Either::Left(_) = future::select(pin!(stop), pin!(tick)).await {
						break;
					}

					// Reap stale builds if the timeout is set.
					let reap = async {
						server
							.try_reap_builds(heartbeat_timeout)
							.await
							.inspect_err(|error| tracing::error!(%error, "failed to reap builds"))
							.ok();
					};

					// Reenqueue builds that have been erroneously dequeued but not started.
					let reenqueue = async {
						server
							.try_reenqueue_builds(dequeue_timeout)
							.await
							.inspect_err(
								|error| tracing::error!(%error, "failed to reenqueue builds"),
							)
							.ok();
					};
					futures::join!(reap, reenqueue);
				}
			}
		});

		let task = std::sync::Mutex::new(Some(task));
		Ok(Monitor { stop, task })
	}

	async fn try_reap_builds(&self, timeout: std::time::Duration) -> tg::Result<()> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get all builds with heartbeat_at older than now - timeout.
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
					status = 'started';
			"
		);
		let time = time::OffsetDateTime::now_utc() - timeout;
		let params = db::params!(time.format(&Rfc3339).unwrap());
		let builds = connection
			.query_all_into::<Row>(statement, params)
			.await
			.inspect_err(|error| tracing::error!(%error, "failed to perform query"))
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?
			.into_iter()
			.map(|row| row.id);

		// Cancel builds that have missed a heartbeat.
		builds
			.into_iter()
			.map(|id| async move {
				let build = tg::Build::with_id(id);
				build.cancel(self).await
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await
			.map_err(|source| tg::error!(!source, "failed to cancel builds"))?;

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
		self.stop.send(true).ok();
	}

	pub async fn wait(&self) {
		let task = self.task.lock().unwrap().take();
		if let Some(task) = task {
			task.await.ok();
		}
	}
}
