use {
	super::{
		AddRunnerRequestArg, AddRunnerResponseOutput, Operation, RemoveRunnerRequestArg,
		RemoveRunnerResponseOutput, Scheduler, State,
	},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{
		collections::{HashMap, HashSet},
		ops::ControlFlow,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

pub(super) struct Runners {
	pub entries: HashMap<tg::runner::Id, Runner, tg::id::BuildHasher>,
	next_connection_index: u64,
}

pub(super) struct Runner {
	pub borrowable: HashSet<tg::sandbox::Id, tg::id::BuildHasher>,
	pub capacity: tg::runner::control::Capacity,
	pub committed: tg::runner::Capacity,
	pub connection_index: u64,
	pub heartbeat_at: tokio::time::Instant,
	pub heartbeat_index: u64,
	pub host: String,
	pub ready: bool,
	pub requests: usize,
	pub reservations: HashMap<tg::sandbox::Id, Reservation, tg::id::BuildHasher>,
	pub reserved: tg::runner::Capacity,
}

pub(super) struct Reservation {
	pub capacity: tg::runner::Capacity,
	pub source: ReservationSource,
	pub state: ReservationState,
}

pub(super) enum ReservationSource {
	Borrowed,
	Regular,
}

#[derive(Clone, Copy)]
pub(super) enum ReservationState {
	Pending,
	Uncertain,
}

impl Runners {
	pub fn new() -> Self {
		Self {
			entries: HashMap::default(),
			next_connection_index: 0,
		}
	}

	pub fn expired(&self, now: tokio::time::Instant, ttl: Duration) -> Vec<(tg::runner::Id, u64)> {
		self.entries
			.iter()
			.filter(|(_, runner)| now.duration_since(runner.heartbeat_at) > ttl)
			.map(|(id, runner)| (id.clone(), runner.connection_index))
			.collect()
	}

	pub fn next_connection_index(&mut self) -> u64 {
		let connection_index = self.next_connection_index;
		self.next_connection_index = self.next_connection_index.wrapping_add(1);

		connection_index
	}
}

impl State {
	pub(super) fn handle_add_runner_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: AddRunnerRequestArg,
	) {
		let connection_index = self.runners.next_connection_index();
		self.remove_runner(&request.runner);
		let runner = Runner {
			borrowable: HashSet::default(),
			capacity: request.capacity,
			committed: tg::runner::Capacity::default(),
			connection_index,
			heartbeat_at: tokio::time::Instant::now(),
			heartbeat_index: 0,
			host: request.host.clone(),
			ready: false,
			requests: 0,
			reservations: HashMap::default(),
			reserved: tg::runner::Capacity::default(),
		};
		self.runners.entries.insert(request.runner.clone(), runner);
		self.queue.wake();

		let scheduler = scheduler.clone();
		let runner = request.runner.clone();
		self.operations.push(
			async move {
				let result = scheduler.add_runner(connection_index, request).await;
				Operation::AddRunner {
					connection_index,
					id,
					result,
					runner,
				}
			}
			.boxed(),
		);
	}

	pub(super) fn handle_remove_runner_request(
		&mut self,
		scheduler: &Scheduler,
		id: Option<String>,
		request: RemoveRunnerRequestArg,
	) {
		let current = self
			.runners
			.entries
			.get(&request.runner)
			.is_some_and(|runner| runner.connection_index == request.connection_index);
		if !current {
			if let Some(id) = id {
				let output = RemoveRunnerResponseOutput {
					runner: request.runner,
				};
				let response =
					Scheduler::response(id, Ok(super::ResponseOutput::RemoveRunner(output)));
				scheduler.send_response(self, response);
			}
			return;
		}
		self.remove_runner(&request.runner);
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler.remove_runner(request).boxed().await;
				Operation::RemoveRunner { id, result }
			}
			.boxed(),
		);
	}
}

impl Scheduler {
	async fn add_runner(
		&self,
		connection_index: u64,
		request: AddRunnerRequestArg,
	) -> tg::Result<AddRunnerResponseOutput> {
		// Upsert the runner into the database.
		let index_arg = tangram_index::batch::Arg {
			put_runners: vec![tangram_index::runner::put::Arg {
				id: request.runner.clone(),
				scheduler: Some(self.id.clone()),
			}],
			..Default::default()
		};
		let scheduler_id = self.id.clone();
		let server = self.server.clone();
		let runner_id = request.runner.clone();
		self.server
			.database
			.run(|transaction| {
				let index_arg = index_arg.clone();
				let scheduler_id = scheduler_id.clone();
				let server = server.clone();
				let runner_id = runner_id.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							insert into runners (id, scheduler, status)
							values ({p}1, {p}2, {p}3)
							on conflict (id) do update set scheduler = {p}2, status = {p}3;
						"
					);
					let params =
						db::params![runner_id.to_string(), scheduler_id.to_string(), "started"];
					let result = transaction.execute(statement.into(), params).await;
					crate::database::retry!(result, "failed to execute the statement");
					server
						.enqueue_database_outbox_with_transaction(transaction, &index_arg)
						.await?;
					Ok::<_, tg::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to upsert the runner"))?;

		let output = AddRunnerResponseOutput {
			connection_index,
			runner: request.runner,
		};

		Ok(output)
	}

	async fn remove_runner(
		&self,
		request: RemoveRunnerRequestArg,
	) -> tg::Result<RemoveRunnerResponseOutput> {
		self.server
			.cleanup_lost_runner(&request.runner, &self.id)
			.boxed()
			.await
			.map_err(
				|error| tg::error!(!error, runner = %request.runner, "failed to clean up the lost runner"),
			)?;

		// Mark the runner as stopped and clear its scheduler in the database.
		let index_arg = tangram_index::batch::Arg {
			put_runners: vec![tangram_index::runner::put::Arg {
				id: request.runner.clone(),
				scheduler: None,
			}],
			..Default::default()
		};
		let runner_id = request.runner.clone();
		let scheduler_id = self.id.clone();
		let server = self.server.clone();
		self.server
			.database
			.run(|transaction| {
				let index_arg = index_arg.clone();
				let runner_id = runner_id.clone();
				let scheduler_id = scheduler_id.clone();
				let server = server.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							update runners
							set status = {p}1, scheduler = null
							where id = {p}2 and scheduler = {p}3;
						"
					);
					let params =
						db::params!["stopped", runner_id.to_string(), scheduler_id.to_string(),];
					let result = transaction.execute(statement.into(), params).await;
					crate::database::retry!(result, "failed to execute the statement");
					server
						.enqueue_database_outbox_with_transaction(transaction, &index_arg)
						.await?;
					Ok::<_, tg::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to update the runner"))?;

		let output = RemoveRunnerResponseOutput {
			runner: request.runner,
		};

		Ok(output)
	}
}
