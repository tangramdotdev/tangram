use {
	crate::{Server, database},
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt, TryStreamExt, future,
		stream::FuturesUnordered,
	},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{collections::BTreeMap, pin::pin},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_messenger::{BatchConfig, prelude::*},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub async fn watchdog_task(&self, config: &crate::config::Watchdog) {
		let expired_processes = tokio::spawn({
			let server = self.clone();
			let config = config.clone();
			async move { server.watchdog_heartbeat_task(&config).await }
		});
		let process_cycles = tokio::spawn({
			let server = self.clone();
			let config = config.clone();
			async move { server.watchdog_cycle_task(&config).await }
		});
		match future::select(expired_processes, process_cycles).await {
			future::Either::Left((result, task)) => {
				if let Err(error) = result {
					tracing::error!(?error, "watchdog task panicked");
				}
				task.await
					.inspect_err(|error| tracing::error!(?error, "watchdog task panicked"))
					.ok();
			},
			future::Either::Right((result, task)) => {
				if let Err(error) = result {
					tracing::error!(?error, "watchdog task panicked");
				}
				task.await
					.inspect_err(|error| tracing::error!(?error, "watchdog task panicked"))
					.ok();
			},
		}
	}

	async fn watchdog_heartbeat_task(&self, config: &crate::config::Watchdog) {
		loop {
			// Finish processes.
			let result = self
				.watchdog_heartbeat_task_inner(config)
				.inspect_err(
					|error| tracing::error!(error = %error.trace(), "failed to finish processes"),
				)
				.await;

			// If an error occurred or no processes were finished, wait to be signaled or for the timeout to expire.
			if matches!(result, Err(_) | Ok(0)) {
				let Ok(stream) = self
					.messenger
					.subscribe::<()>("watchdog".into(), None)
					.await
					.inspect_err(|error| {
						tracing::error!(
							?error,
							"failed to subscribe to the watchdog message stream"
						);
					})
				else {
					tokio::time::sleep(config.interval).await;
					continue;
				};
				let mut stream = pin!(stream);
				tokio::time::timeout(config.interval, stream.next())
					.await
					.ok();
			}
		}
	}

	async fn watchdog_heartbeat_task_inner(
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
						error: Some(tg::Either::Left(error)),
						exit: 1,
						local: None,
						output: None,
						remotes: None,
					};
					server
						.finish_process(&row.id, arg)
						.await
						.inspect_err(|error| {
							tracing::error!(error = %error.trace(), "failed to cancel the process");
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

	async fn watchdog_cycle_task(&self, config: &crate::config::Watchdog) {
		// Subscribe to the consumer stream.
		let Ok(stream) = self
			.messenger
			.get_stream("queue".into())
			.await
			.inspect_err(|error| tracing::error!(?error, "failed to get the stream"))
		else {
			return;
		};
		let Ok(consumer) = stream
			.get_consumer("watchdog".into())
			.await
			.inspect_err(|error| tracing::error!(?error, "failed to get the consumer"))
		else {
			return;
		};
		let Ok(stream) = consumer
			.batch_subscribe::<crate::process::queue::Message>(BatchConfig {
				max_messages: Some(config.batch_size.to_u64().unwrap()),
				max_bytes: None,
				timeout: Some(config.ttl),
			})
			.await
			.inspect_err(|error| tracing::error!(?error, "failed to subscribe"))
		else {
			return;
		};
		let stream = stream.ready_chunks(config.batch_size);
		let mut stream = pin!(stream);
		let mut messages = Vec::with_capacity(config.batch_size);
		let mut ackers = Vec::with_capacity(config.batch_size);
		while let Some(chunk) = stream.next().await {
			messages.clear();
			ackers.clear();
			for result in chunk {
				let Ok(message) = result.inspect_err(|error| {
					tracing::error!(?error, "error reading from process queue stream");
				}) else {
					continue;
				};
				let (message, acker) = message.split();
				messages.push(message);
				ackers.push(acker);
			}
			self.watchdog_cycle_task_inner(&messages)
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to finish process cycles"))
				.ok();
			while let Some(acker) = ackers.pop() {
				acker.ack().await.ok();
			}
		}
	}

	async fn watchdog_cycle_task_inner(
		&self,
		messages: &[crate::process::queue::Message],
	) -> tg::Result<()> {
		let mut connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let results = match &transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(transaction) => {
				Self::get_process_cycles_postgres(transaction, messages)
					.await
					.map_err(|source| tg::error!(!source, "failed to get process cycles"))?
			},
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(transaction) => {
				Self::get_process_cycles_sqlite(transaction, messages)
					.await
					.map_err(|source| tg::error!(!source, "failed to get process cycles"))?
			},
		};

		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		drop(connection);

		results
			.into_iter()
			.map(|(id, error)| {
				let server = self.clone();
				async move {
					let arg = tg::process::finish::Arg {
						checksum: None,
						error: Some(tg::Either::Left(error)),
						exit: 1,
						local: None,
						output: None,
						remotes: None,
					};
					server.finish_process(&id, arg).await
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await
			.map_err(|source| tg::error!(!source, "failed to finish processes"))?;

		Ok(())
	}
}
