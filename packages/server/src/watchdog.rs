use {
	crate::Server,
	futures::StreamExt as _,
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) fn spawn_publish_watchdog_message_task(&self) {
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish("watchdog".into(), ())
					.await
					.inspect_err(|error| {
						tracing::error!(?error, "failed to publish the watchdog message");
					})
					.ok();
			}
		});
	}

	pub async fn watchdog_task(&self, config: &crate::config::Watchdog) -> tg::Result<()> {
		loop {
			// Finish processes.
			let result = self.watchdog_task_inner(config).await.inspect_err(
				|error| tracing::error!(error = %error.trace(), "failed to finish processes"),
			);

			// If an error occurred or no processes were finished, wait to be signaled or for the timeout to expire.
			if matches!(result, Err(_) | Ok(0)) {
				let stream = self
					.messenger
					.subscribe::<()>("watchdog".into(), None)
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
		let connection =
			self.process_store.connection().await.map_err(|source| {
				tg::error!(!source, "failed to get a process store connection")
			})?;
		let p = connection.p();

		// Get processes to finish.
		#[derive(Debug, db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::sandbox::Id,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			code: Option<tg::error::Code>,
			message: String,
		}
		let statement = formatdoc!(
			"
				select id, null as code, 'maximum depth exceeded' as message
				from processes
				where status != 'finished' and depth > {p}1

				union all

				select id, 'cancellation' as code, 'the process was canceled' as message
				from processes
				where status != 'finished' and token_count = 0

				union all

				select id, 'heartbeat_expiration' as code, 'heartbeat expired' as message
				from sandboxes
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

		drop(connection);

		// let rows = rows
		// 	.into_iter()
		// 	.fold(BTreeMap::<tg::sandbox::Id, Row>::new(), |mut rows, row| {
		// 		rows.entry(row.id.clone()).or_insert(row);
		// 		rows
		// 	})
		// 	.into_values()
		// 	.collect::<Vec<_>>();
		// let results = rows
		// 	.into_iter()
		// 	.map(|row| {
		// 		let server = self.clone();
		// 		async move {
		// 			let claimed_for_cancellation =
		// 				matches!(row.code, Some(tg::error::Code::Cancellation));
		// 			if claimed_for_cancellation {
		// 				let ready = server
		// 					.watchdog_try_finish_sandbox(&row.id)
		// 					.await
		// 					.inspect_err(|error| {
		// 						tracing::error!(error = %error.trace(), "failed to claim the tokenless sandbox for cancellation");
		// 					})
		// 						.unwrap_or(false);
		// 				if !ready {
		// 					return false;
		// 				}
		// 			}
		// 			let error = tg::error::Data {
		// 				code: row.code,
		// 				diagnostics: None,
		// 				location: None,
		// 				message: Some(row.message),
		// 				source: None,
		// 				stack: None,
		// 				values: BTreeMap::default(),
		// 			};
		// 			server
		// 				.finish_unfinished_processes_in_sandbox_local(&row.id, tg::Either::Left(error))
		// 				.await
		// 				.inspect_err(|error| {
		// 					tracing::error!(error = %error.trace(), "failed to cancel the sandbox processes");
		// 				})
		// 				.ok();
		// 			if claimed_for_cancellation {
		// 				server
		// 					.enqueue_finished_sandbox_local(&row.id)
		// 					.await
		// 					.inspect_err(|error| {
		// 						tracing::error!(error = %error.trace(), "failed to enqueue sandbox finalization");
		// 					})
		// 					.ok();
		// 			} else {
		// 				server
		// 					.try_finish_sandbox_local(&row.id)
		// 					.await
		// 					.inspect_err(|error| {
		// 						tracing::error!(error = %error.trace(), "failed to finish the sandbox");
		// 					})
		// 					.ok();
		// 			}
		// 			true
		// 		}
		// 		.boxed()
		// 	})
		// 	.collect::<FuturesUnordered<_>>()
		// 	.collect::<Vec<_>>()
		// 	.await;
		// let n = results
		// 	.into_iter()
		// 	.filter(|result| *result)
		// 	.count()
		// 	.to_u64()
		// 	.unwrap();

		// Ok(n)

		todo!()
	}

	// pub(crate) async fn watchdog_try_finish_sandbox(
	// 	&self,
	// 	id: &tg::sandbox::Id,
	// ) -> tg::Result<bool> {
	// 	let connection = self
	// 		.process_store
	// 		.write_connection()
	// 		.await
	// 		.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;
	// 	let p = connection.p();
	// 	let statement = formatdoc!(
	// 		"
	// 			update sandboxes
	// 			set
	// 				finished_at = {p}1,
	// 				heartbeat_at = null,
	// 				status = 'finished'
	// 			where
	// 				id = {p}2 and
	// 				status != 'finished' and
	// 				not exists (
	// 					select 1
	// 					from processes
	// 					where
	// 						sandbox = {p}2 and
	// 						status != 'finished' and
	// 						token_count > 0
	// 				);
	// 		"
	// 	);
	// 	let now = time::OffsetDateTime::now_utc().unix_timestamp();
	// 	let params = db::params![now, id.to_string()];
	// 	let n = connection
	// 		.execute(statement.into(), params)
	// 		.await
	// 		.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
	// 	drop(connection);
	// 	if n == 0 {
	// 		return Ok(false);
	// 	}
	// 	self.spawn_publish_sandbox_status_task(id);
	// 	Ok(true)
	// }
}
