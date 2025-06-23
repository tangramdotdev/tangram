use crate::Server;
use futures::{FutureExt, StreamExt as _, stream::FuturesUnordered};
use indoc::formatdoc;
use num::ToPrimitive as _;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_messenger::Messenger as _;
// use tokio_stream::StreamExt;
use std::pin::pin;

impl Server {
	pub async fn watchdog_task(&self, config: &crate::config::Watchdog) -> tg::Result<()> {
		// Subscribe to the cancellation stream.
		let mut stream = self
			.messenger
			.subscribe("processes.canceled".into(), None)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to subscribe to the cancellation message stream"
				)
			})?;
		let mut stream = pin!(stream);
		loop {
			// Try and reap processes.
			let result = self
				.watchdog_task_inner(config)
				.await
				.inspect_err(|error| tracing::error!(%error, "failed to cancel processes"));

			// If nothing was reaped, wait to be signaled or the timeout to expire.
			if matches!(result, Err(_) | Ok(0)) {
				tokio::time::timeout(config.interval, stream.next())
					.await
					.ok();
			}
		}
	}

	pub(crate) async fn watchdog_task_inner(
		&self,
		config: &crate::config::Watchdog,
	) -> tg::Result<u64> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();

		// Get processes to finish.
		#[derive(Debug, serde::Deserialize)]
		struct Row {
			id: tg::process::Id,
			code: Option<tg::error::Code>,
			message: String,
		}
		let statement = formatdoc!(
			"
				select id, null, 'maximum depth exceeded'
				from processes
				where status = 'started' and depth > {p}1

				union all

				select id, 'cancellation', 'the process was canceled'
				from processes
				where status != 'finished' and token_count = 0

				union all

				select id, null, 'heartbeat expired'
				from processes
				where status = 'started' and heartbeat_at <= {p}1

				limit {p}2;
			"
		);
		let max_depth = config.max_depth;
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let max_heartbeat_at = now - config.ttl.as_secs().to_i64().unwrap();
		let params = db::params![max_depth, max_heartbeat_at, config.batch_size];
		let rows = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Finish the processes.
		let n = rows.len().to_u64().unwrap();
		rows.into_iter()
			.map(|row| {
				let server = self.clone();
				async move {
					let error = tg::Error {
						code: row.code,
						message: Some(row.message),
						..Default::default()
					};
					let error = Some(error.to_data());
					let arg = tg::process::finish::Arg {
						checksum: None,
						error,
						exit: 1,
						force: false,
						output: None,
						remote: None,
					};
					server
						.finish_process(&row.id, arg)
						.await
						.inspect_err(|error| {
							tracing::error!(?error, "failed to cancel the process");
						})
						.ok();
				}
				.boxed()
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		Ok(n)
	}
}
