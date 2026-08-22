use {
	super::{
		AddRunnerRequestArg, AddRunnerResponseOutput, Operation, RemoveRunnerRequestArg,
		RemoveRunnerResponseOutput, Scheduler, State,
	},
	crate::Server,
	futures::FutureExt as _,
	std::{
		collections::{HashMap, HashSet},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
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
	pub owner: Option<tg::Id>,
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
		let completions = self.remove_runner(&request.runner);
		scheduler.send_dequeue_sandbox_completions(self, completions);
		let runner = Runner {
			borrowable: HashSet::default(),
			capacity: request.capacity,
			committed: tg::runner::Capacity::default(),
			connection_index,
			heartbeat_at: tokio::time::Instant::now(),
			heartbeat_index: 0,
			host: request.host.clone(),
			owner: None,
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
					scheduler.response(id, Ok(super::ResponseOutput::RemoveRunner(output)));
				scheduler.send_response(self, response);
			}
			return;
		}
		let completions = self.remove_runner(&request.runner);
		scheduler.send_dequeue_sandbox_completions(self, completions);
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
	) -> tg::Result<(AddRunnerResponseOutput, Option<tg::Id>)> {
		let runner = self
			.server
			.session(&self.server.context)
			.try_get_runner_data(&request.runner)
			.await?;
		let owner = match runner {
			Some(runner) => runner.owner.and_then(|owner| owner.to_id()),
			None if self
				.server
				.config
				.roles
				.contains(&crate::config::Role::Runner)
				&& self.server.config.runner.id.is_none()
				&& self.server.runner.state().id().as_ref() == Some(&request.runner) =>
			{
				None
			},
			None => {
				return Err(tg::error!(runner = %request.runner, "failed to find the runner"));
			},
		};
		let output = AddRunnerResponseOutput {
			connection_index,
			runner: request.runner,
			scheduler: self.id.clone(),
		};

		Ok((output, owner))
	}

	async fn remove_runner(
		&self,
		request: RemoveRunnerRequestArg,
	) -> tg::Result<RemoveRunnerResponseOutput> {
		self.server
			.handle_expired_runner(&request.runner)
			.boxed()
			.await
			.map_err(
				|error| tg::error!(!error, runner = %request.runner, "failed to handle the expired runner"),
			)?;

		let output = RemoveRunnerResponseOutput {
			runner: request.runner,
		};

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_expired_runner(&self, runner: &tg::runner::Id) -> tg::Result<()> {
		let sandboxes =
			self.index.get_runner_sandboxes(runner).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"),
			)?;
		for sandbox in sandboxes {
			self.destroy_expired_runner_sandbox(&sandbox).await?;
		}

		Ok(())
	}

	pub(crate) async fn destroy_expired_runner_sandbox(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let now = self.clock.unix_timestamp()?;
		let error = tg::error::Data {
			code: Some(tg::error::Code::HeartbeatExpiration),
			message: Some("heartbeat expired".to_owned()),
			..Default::default()
		};
		let processes =
			self.index.get_sandbox_processes(id).await.map_err(
				|source| tg::error!(!source, %id, "failed to get the sandbox processes"),
			)?;
		for (process, indexed) in processes {
			let mut data = indexed
				.data
				.ok_or_else(|| tg::error!(%process, "missing the process data"))?;
			let finish = data.status.is_started();
			if finish {
				data.children.get_or_insert_default();
				data.cacheable = false;
				data.error = Some(tg::Either::Left(error.clone()));
				data.exit = Some(1);
				data.finished_at = Some(now);
				data.status = tg::process::Status::Finished;
				let mut context = self.context.clone();
				context.principal = tg::Principal::Process(process.clone());
				let session = self.session(&context);
				session
					.put_process_local(
						&process,
						tg::process::put::Arg {
							data: data.clone(),
							location: None,
						},
						true,
					)
					.boxed()
					.await
					.map_err(
						|source| tg::error!(!source, %process, "failed to store the finished process"),
					)?;
				let session = self.session(&self.context);
				session.spawn_process_finish_tasks(&process);
			} else {
				// Remove the tokens before updating the index.
				let data = data.without_location_and_tokens();

				self.index
					.batch(tangram_index::batch::Arg {
						items: vec![tangram_index::batch::Item::PutProcess(
							tangram_index::process::put::Arg {
								cached: false,
								children: None,
								command: data.command.node.clone().into(),
								data: Some(data.clone()),
								error: None,
								id: process.clone(),
								log: None,
								metadata: indexed.metadata,
								options: tg::referent::Options::default(),
								output: None,
								parent: None,
								sandbox: Some(data.sandbox.clone()),
								stored: indexed.stored,
								time_to_touch: self.config.process.time_to_touch,
								touched_at: now,
							},
						)],
					})
					.await
					.map_err(
						|source| tg::error!(!source, %process, "failed to update the process in the index"),
					)?;
			}
		}

		let mut indexed = self
			.index
			.try_get_sandbox(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox from the index"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox in the index"))?;
		let data = indexed
			.data
			.as_mut()
			.ok_or_else(|| tg::error!(%id, "missing the sandbox data"))?;
		data.status = tg::sandbox::Status::Destroyed;
		let account = if self.config.usage.enabled {
			indexed.account
		} else {
			None
		};
		self.index
			.batch(tangram_index::batch::Arg {
				items: vec![tangram_index::batch::Item::PutSandbox(
					tangram_index::sandbox::put::Arg {
						account,
						created_at: indexed.created_at,
						data: indexed.data,
						id: id.clone(),
						runner: indexed.runner,
						touched_at: now,
					},
				)],
			})
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to update the destroyed sandbox in the index"),
			)?;
		self.spawn_publish_sandbox_status_task(id);

		Ok(())
	}
}
