use {
	crate::Server,
	dashmap::DashMap,
	futures::{FutureExt as _, StreamExt as _},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{
		ops::{ControlFlow, Deref},
		pin::pin,
		sync::Arc,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::Task,
	tangram_index::prelude::*,
	tangram_messenger::prelude::*,
};

struct Owned {
	task: Task<tg::Result<()>>,
	watchdog: Watchdog,
}

#[derive(Clone)]
struct Watchdog {
	state: Arc<State>,
}

struct State {
	config: crate::config::Watchdog,
	schedulers: DashMap<tg::scheduler::Id, Scheduler>,
	server: Server,
}

struct Scheduler {
	heartbeat_at: tokio::time::Instant,
}

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
			let result = self.watchdog_task_inner(config).await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "the watchdog task failed");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn watchdog_task_inner(&self, config: &crate::config::Watchdog) -> tg::Result<()> {
		let watchdog = Watchdog::start(self, config).await?;
		watchdog.wait().await?;
		Ok(())
	}
}

impl Owned {
	async fn wait(self) -> tg::Result<()> {
		let result = self.task.wait().await;
		if let Err(error) = result {
			return Err(tg::error!(!error, "the watchdog task panicked"));
		}
		Err(tg::error!("the watchdog stopped"))
	}
}

impl Watchdog {
	async fn start(server: &Server, config: &crate::config::Watchdog) -> tg::Result<Owned> {
		// Create the watchdog.
		let state = Arc::new(State {
			config: config.clone(),
			schedulers: DashMap::new(),
			server: server.clone(),
		});
		let watchdog = Self { state };

		// Get the started schedulers.
		let schedulers = server.get_started_schedulers().await?;
		for id in schedulers {
			let scheduler = Scheduler {
				heartbeat_at: tokio::time::Instant::now(),
			};
			watchdog.schedulers.insert(id, scheduler);
		}

		// Spawn the task.
		let task = watchdog.spawn_task();

		let owned = Owned { task, watchdog };

		Ok(owned)
	}

	fn spawn_task(&self) -> Task<tg::Result<()>> {
		let watchdog = self.clone();
		Task::spawn(move |_| async move { watchdog.task().await })
	}

	async fn task(&self) -> tg::Result<()> {
		let _heartbeat_task = self.spawn_scheduler_heartbeat_task();
		let wakeups = self
			.server
			.messenger
			.subscribe::<()>("watchdog".into())
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to subscribe to the watchdog message stream")
			})?;
		let mut wakeups = pin!(wakeups);
		loop {
			match self.task_inner().await {
				Ok(0) => {
					tokio::time::timeout(self.config.interval, wakeups.next())
						.await
						.ok();
				},
				Ok(_) => {},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to run the watchdog");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	async fn task_inner(&self) -> tg::Result<u64> {
		let mut count = 0u64;

		// Handle processes.
		count += self.handle_processes().await?;

		// Handle expired schedulers.
		count += self.handle_schedulers().await?;

		Ok(count)
	}

	/// Finish processes that have exceeded the maximum depth.
	async fn handle_processes(&self) -> tg::Result<u64> {
		// Finish processes that have exceeded the maximum depth.
		let processes = self
			.server
			.index
			.get_process_depth_detections(self.config.batch_size)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the process depth detections"))?;
		let count = processes.len().to_u64().unwrap();
		for id in processes {
			let error = tg::error::Data {
				message: Some("maximum depth exceeded".into()),
				..Default::default()
			};
			let request = tg::process::control::ServerRequestArg::Finish(
				tg::process::control::FinishServerRequestArg {
					error: Some(error),
					exit: 1,
				},
			);
			let options = crate::control::Options {
				retry: tangram_futures::retry::Options::default(),
				timeout: Duration::from_secs(10),
			};
			let session = self.server.session(&self.server.context);
			let wait_future = session
				.try_wait_process_future(&id, tg::process::wait::Arg::default())
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?
				.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
			let finish_future = session.send_process_control_request(&id, request, options);
			let mut wait_future = pin!(wait_future);
			let mut finish_future = pin!(finish_future);
			tokio::select! {
				output = &mut wait_future => {
					let output = output
						.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?;
					if output.is_none() {
						return Err(tg::error!(%id, "the process wait ended without output"));
					}
				},
				response = &mut finish_future => {
					let response = response
						.map_err(
							|error| tg::error!(!error, %id, "failed to send the finish process control request"),
						)?
						.map_err(
							|error| tg::error!(!error, %id, "the finish process control request failed"),
						)?;
					response
						.try_unwrap_finish()
						.map_err(|_| tg::error!(%id, "expected a finish response"))?;
				},
			}
		}

		Ok(count)
	}

	async fn handle_schedulers(&self) -> tg::Result<u64> {
		let now = tokio::time::Instant::now();
		let ttl = self.config.ttl;

		// Get schedulers whose heartbeats have expired.
		let expired_schedulers = self
			.schedulers
			.iter()
			.filter(|entry| now.duration_since(entry.value().heartbeat_at) > ttl)
			.map(|entry| entry.key().clone())
			.collect::<Vec<_>>();
		if expired_schedulers.is_empty() {
			return Ok(0);
		}

		let mut handled = Vec::new();
		for scheduler in expired_schedulers {
			let runners = self
				.server
				.index
				.get_scheduler_runners(&scheduler)
				.await
				.map_err(
					|error| tg::error!(!error, %scheduler, "failed to get the scheduler runners"),
				)?;
			let mut failed = false;
			for runner in runners {
				if let Err(error) = self.server.cleanup_lost_runner(&runner, &scheduler).await {
					tracing::error!(error = %error.trace(), %runner, %scheduler, "failed to clean up the lost runner");
					failed = true;
					break;
				}
			}
			if !failed {
				handled.push(scheduler);
			}
		}
		if handled.is_empty() {
			return Ok(0);
		}

		let handled_for_transaction = handled.clone();
		let watchdog = self.clone();
		self.server
			.database
			.run(|transaction| {
				let handled = handled_for_transaction.clone();
				let watchdog = watchdog.clone();
				async move {
					watchdog
						.handle_expired_schedulers_with_transaction(transaction, &handled)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to handle expired schedulers"))?;
		for scheduler in &handled {
			self.schedulers.remove(scheduler);
		}

		Ok(handled.len().to_u64().unwrap())
	}

	async fn handle_expired_schedulers_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		expired_schedulers: &[tg::scheduler::Id],
	) -> Result<ControlFlow<(), crate::database::Error>, tg::Error> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::runner::Id,
		}

		let mut index_arg = tangram_index::batch::Arg::default();
		for scheduler in expired_schedulers {
			let p = transaction.p();
			let statement = formatdoc!(
				"
					update schedulers
					set status = {p}1
					where id = {p}2 and status = {p}3;
				"
			);
			let params = db::params!["stopped", scheduler.to_string(), "started",];
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to execute the statement");

			let statement = formatdoc!(
				"
					update runners
					set status = {p}1, scheduler = null
					where scheduler = {p}2 and status = {p}3
					returning id;
				"
			);
			let params = db::params!["stopped", scheduler.to_string(), "started",];
			let result = transaction
				.query_all_into::<Row>(statement.into(), params)
				.await;
			let rows = crate::database::retry!(result, "failed to execute the statement");
			index_arg.items.extend(rows.into_iter().map(|row| {
				tangram_index::batch::Item::PutRunner(tangram_index::runner::put::Arg {
					id: row.id,
					scheduler: None,
				})
			}));
		}
		self.server
			.enqueue_database_outbox_with_transaction(transaction, &index_arg)
			.await?;

		Ok(ControlFlow::Break(()))
	}

	fn spawn_scheduler_heartbeat_task(&self) -> Task<tg::Result<()>> {
		let watchdog = self.clone();
		Task::spawn(async move |_| {
			watchdog
				.scheduler_heartbeat_task()
				.await
				.inspect_err(|error| {
					tracing::error!(error = %error.trace(), "the watchdog heartbeat task failed");
				})
		})
	}

	async fn scheduler_heartbeat_task(&self) -> tg::Result<()> {
		let stream = self
			.server
			.messenger
			.subscribe::<tangram_messenger::payload::Json<tg::scheduler::Id>>(
				"schedulers.heartbeat".into(),
			)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to subscribe to the scheduler heartbeat stream"
				)
			})?;
		let mut stream = pin!(stream);
		while let Some(Ok(message)) = stream.next().await {
			let scheduler = Scheduler {
				heartbeat_at: tokio::time::Instant::now(),
			};
			self.schedulers.insert(message.payload.0, scheduler);
		}
		Ok(())
	}
}

impl Deref for Owned {
	type Target = Watchdog;

	fn deref(&self) -> &Self::Target {
		&self.watchdog
	}
}

impl Deref for Watchdog {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		self.state.as_ref()
	}
}
