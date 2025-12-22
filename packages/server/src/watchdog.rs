use {
	crate::Server,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{collections::BTreeMap, pin::pin},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_either::Either,
	tangram_messenger::prelude::*,
};

impl Server {
	pub async fn watchdog_task(&self, config: &crate::config::Watchdog) -> tg::Result<()> {
		loop {
			// Reap processes.
			let result = self
				.watchdog_task_inner(config)
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to cancel processes"));

			// If an error occurred or no processes were reaped, wait to be signaled or for the timeout to expire.
			if matches!(result, Err(_) | Ok(0)) {
				let stream = self
					.messenger
					.subscribe("watchdog".into(), None)
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to subscribe to the cancellation message stream"
						)
					})?;
				let mut stream = pin!(stream);
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
		#[derive(Debug, db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::process::Id,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			code: Option<tg::error::Code>,
			message: String,
		}
		let statement = formatdoc!(
			"
				select id, null as code, 'maximum depth exceeded' as message
				from processes
				where status = 'started' and depth > {p}1

				union all

				select id, 'cancellation' as code, 'the process was canceled' as message
				from processes
				where status != 'finished' and token_count = 0

				union all

				select id, 'heartbeat_expiration' as code, 'heartbeat expired' as message
				from processes
				where status = 'started' and heartbeat_at < {p}2

				limit {p}3;
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
					let error = tg::error::Data {
						code: row.code,
						diagnostics: None,
						location: None,
						message: Some(row.message),
						source: None,
						stack: None,
						values: BTreeMap::default(),
					};
					let arg = tg::process::finish::Arg {
						checksum: None,
						error: Some(Either::Left(error)),
						exit: 1,
						local: None,
						output: None,
						remotes: None,
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
