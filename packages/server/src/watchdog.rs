use crate::Server;
use futures::{StreamExt as _, stream::FuturesUnordered};
use indoc::formatdoc;
use num::ToPrimitive as _;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub async fn watchdog_task(&self, config: &crate::config::Watchdog) {
		loop {
			let result = self
				.watchdog_task_inner(config)
				.await
				.inspect_err(|error| tracing::error!(%error, "failed to cancel processes"));
			if matches!(result, Err(_) | Ok(0)) {
				tokio::time::sleep(config.interval).await;
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
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get all started processes whose heartbeat_at exceeds the timeout.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update processes
				set status = 'started'
				from (
					select id
					from processes
					where
						status = 'started' and
						heartbeat_at <= {p}1
					limit {p}2
				) as updates
				where processes.id = updates.id
				returning id;
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let max_heartbeat_at = now - config.ttl.as_secs().to_i64().unwrap();
		let params = db::params![max_heartbeat_at, config.batch_size];
		let timeout_exceeded_processes = connection
			.query_all_value_into::<tg::process::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Get all started processes whose depth exceeds the limit.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select id
				from processes
				where
					status = 'started' and
					depth > {p}1
				limit {p}2
			"
		);
		let max_depth = config.max_depth;
		let params = db::params![max_depth, config.batch_size];
		let depth_exceeded_processes = connection
			.query_all_value_into::<tg::process::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Cancel the processes.
		timeout_exceeded_processes
			.iter()
			.map(|process| {
				let server = self.clone();
				async move {
					let error = Some(
						tg::error!(
							code = tg::error::Code::Cancelation,
							"the process's heartbeat expired"
						)
						.to_data(),
					);
					let arg = tg::process::finish::Arg {
						checksum: None,
						error,
						exit: 1,
						force: false,
						output: None,
						remote: None,
					};
					server
						.finish_process(process, arg)
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

		depth_exceeded_processes
			.iter()
			.map(|process| {
				let server = self.clone();
				async move {
					let error = Some(tg::error!(
						code = tg::error::Code::Cancelation,
						"the process's depth exceeded the limit"
					));
					let arg = tg::process::finish::Arg {
						checksum: None,
						error,
						exit: 1,
						force: false,
						output: None,
						remote: None,
					};
					server
						.finish_process(process, arg)
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

		Ok(timeout_exceeded_processes.len().to_u64().unwrap()
			+ depth_exceeded_processes.len().to_u64().unwrap())
	}
}
