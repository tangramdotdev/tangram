use {
	crate::Server,
	futures::{StreamExt as _, TryStreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{collections::BTreeSet, pin::pin},
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

		// Get entries to finish.
		#[derive(Debug, db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::value::FromStr>")]
			code: Option<tg::error::Code>,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Either<tg::process::Id, tg::sandbox::Id>,
			message: String,
		}
		let statement = formatdoc!(
			"
				select id, code, message
				from (
					select 0 as priority, id, null as code, 'maximum depth exceeded' as message
					from processes
					where status != 'finished' and depth > {p}1

					union all

					select 1 as priority, id, 'cancellation' as code, 'the process was canceled' as message
					from processes
					where status != 'finished' and token_count = 0

					union all

					select 2 as priority, id, 'heartbeat_expiration' as code, 'heartbeat expired' as message
					from sandboxes
					where status = 'started' and heartbeat_at < {p}2
				) as entries
				order by priority, id
				limit {p}3;
			"
		);
		let max_depth = config.max_depth.to_i64().unwrap();
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let max_heartbeat_at = now - config.ttl.as_secs().to_i64().unwrap();
		let batch_size = config.batch_size.to_i64().unwrap();
		let params = db::params![max_depth, max_heartbeat_at, batch_size];
		let rows = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		drop(connection);

		let mut seen = BTreeSet::new();
		let rows = rows
			.into_iter()
			.filter(|row| seen.insert(row.id.clone()))
			.collect::<Vec<_>>();
		let results = rows
			.into_iter()
			.map(|entry| {
				let server = self.clone();
				async move {
					let error = tg::error::Data {
						code: entry.code,
						message: Some(entry.message),
						..Default::default()
					};
					match entry.id {
						tg::Either::Left(id) => {
							let condition = match entry.code {
								None => {
									crate::process::finish::Condition::DepthExceeded { max_depth }
								},
								Some(tg::error::Code::Cancellation) => {
									crate::process::finish::Condition::TokenCountZero
								},
								Some(code) => {
									return Err(
										tg::error!(%code, "invalid process finish condition"),
									);
								},
							};
							let arg = tg::process::finish::Arg {
								checksum: None,
								error: Some(tg::Either::Left(error)),
								exit: 1,
								location: Some(
									tg::Location::Local(tg::location::Local::default()).into(),
								),
								output: None,
							};
							let finished = server
								.try_finish_process_local(&id, arg, Some(condition))
								.await
								.map_err(
									|source| tg::error!(!source, %id, "failed to finish the process"),
								)?
								.unwrap_or(false);
							Ok::<_, tg::Error>(finished)
						},
						tg::Either::Right(id) => {
							match entry.code {
								Some(tg::error::Code::HeartbeatExpiration) => (),
								Some(code) => {
									return Err(
										tg::error!(%code, "invalid sandbox finish condition"),
									);
								},
								None => {
									return Err(tg::error!("invalid sandbox finish condition"));
								},
							}
							let condition = crate::sandbox::finish::Condition::HeartbeatExpired {
								max_heartbeat_at,
							};
							let finished = server
								.try_finish_sandbox_local(
									&id,
									Some(tg::Either::Left(error)),
									Some(condition),
								)
								.await
								.map_err(
									|source| tg::error!(!source, %id, "failed to finish the sandbox"),
								)?
								.unwrap_or(false);
							Ok::<_, tg::Error>(finished)
						},
					}
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		let n = results
			.into_iter()
			.filter(|result| *result)
			.count()
			.to_u64()
			.unwrap();

		Ok(n)
	}
}
