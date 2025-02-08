use crate::Server;
use futures::{stream::FuturesUnordered, StreamExt as _};
use num::ToPrimitive as _;
use std::time::Duration;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn watchdog_task(&self, options: &crate::config::Watchdog) {
		loop {
			let result = self
				.watchdog_task_inner(options.timeout, options.limit)
				.await
				.inspect_err(|error| tracing::error!(%error, "failed to cancel processes"));
			if matches!(result, Err(_) | Ok(0)) {
				tokio::time::sleep(options.interval).await;
			}
		}
	}

	pub(crate) async fn watchdog_task_inner(
		&self,
		timeout: Duration,
		limit: usize,
	) -> tg::Result<u64> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get all started processes whose heartbeat_at exceeds the timeout.
		let p = connection.p();
		let statement = format!(
			"
				select id
				from processes
				where
					(status = 'started' or status = 'finishing') and
					heartbeat_at <= {p}1
				limit {p}2;
			"
		);
		let time = (time::OffsetDateTime::now_utc() - timeout)
			.format(&Rfc3339)
			.unwrap();
		let params = db::params![time, limit];
		let processes = connection
			.query_all_value_into::<tg::process::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Cancel the processes.
		processes
			.iter()
			.map(|process| {
				let server = self.clone();
				async move {
					let error = Some(tg::error!("the process's heartbeat expired"));
					let arg = tg::process::finish::Arg {
						error,
						exit: None,
						output: None,
						remote: None,
						status: tg::process::Status::Canceled,
					};
					server
						.try_finish_process(process, arg)
						.await
						.inspect_err(|error| {
							tracing::error!(?error, "failed to cancel the process");
						})
						.ok();
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		Ok(processes.len().to_u64().unwrap())
	}
}
