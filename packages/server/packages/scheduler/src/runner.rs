use {
	super::{
		AddRunnerRequestArg, AddRunnerResponseOutput, Operation, RemoveRunnerRequestArg,
		RemoveRunnerResponseOutput, Scheduler, SchedulerState,
	},
	crate::Component,
	futures::FutureExt as _,
	std::{
		collections::{BTreeSet, HashMap, HashSet},
		time::Duration,
	},
	tangram_cache::log,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_server_core::Server,
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
	pub heartbeat_at: tangram_server_core::clock::Instant,
	pub heartbeat_index: u64,
	pub host: String,
	pub owner: Option<tg::Id>,
	pub ready: bool,
	pub requests: usize,
	pub reservations: HashMap<tg::sandbox::Id, Reservation, tg::id::BuildHasher>,
	pub reserved: tg::runner::Capacity,
}

pub(super) struct Reservation {
	/// The capacity that the reservation holds on the runner, which for a borrowed sandbox is only the capacity that it acquires in addition to what it borrows.
	pub capacity: tg::runner::Capacity,
	pub state: ReservationState,
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

	pub fn expired(
		&self,
		now: tangram_server_core::clock::Instant,
		ttl: Duration,
	) -> Vec<(tg::runner::Id, u64)> {
		self.entries
			.iter()
			.filter(|(_, runner)| now - runner.heartbeat_at > ttl)
			.map(|(id, runner)| (id.clone(), runner.connection_index))
			.collect()
	}

	pub fn next_connection_index(&mut self) -> u64 {
		let connection_index = self.next_connection_index;
		self.next_connection_index = self.next_connection_index.wrapping_add(1);

		connection_index
	}
}

impl SchedulerState {
	pub(super) fn handle_add_runner_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: AddRunnerRequestArg,
	) {
		let connection_index = self.runners.next_connection_index();
		tracing::info!(runner = %request.runner, connection_index, "registering the runner connection");
		let completions = self.remove_runner(&request.runner);
		scheduler.send_dequeue_sandbox_completions(self, completions);
		let runner = Runner {
			borrowable: HashSet::default(),
			capacity: request.capacity,
			committed: tg::runner::Capacity::default(),
			connection_index,
			heartbeat_at: scheduler.server.clock.instant(),
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
		tracing::info!(runner = %request.runner, connection_index = request.connection_index, "removing the runner connection");
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
		let session = self.server.session(&self.server.context);
		let runner = self
			.server
			.components
			.runner
			.try_get_runner_data(&session, &request.runner)
			.await?;
		let owner = match runner {
			Some(runner) => runner.owner.and_then(|owner| owner.to_id()),
			None if self
				.server
				.config
				.roles
				.contains(&tangram_server_core::config::Role::Runner)
				&& self.server.config.runner.id.is_none()
				&& self.server.components.runner.state().id().as_ref() == Some(&request.runner) =>
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
		self.component
			.handle_expired_runner(&self.server, &request.runner)
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

impl Component {
	pub(crate) async fn handle_expired_runner(
		&self,
		server: &Server,
		runner: &tg::runner::Id,
	) -> tg::Result<()> {
		let sandboxes =
			server.index.get_runner_sandboxes(runner).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"),
			)?;
		tracing::warn!(%runner, sandboxes = sandboxes.len(), "finishing the expired runner sandboxes");
		tangram_server_core::checkpoint!(server, "scheduler.runner.expired", %runner).await;
		for sandbox in sandboxes {
			let error = tg::error::Data {
				code: Some(tg::error::Code::Internal),
				message: Some("heartbeat expired".to_owned()),
				..Default::default()
			};
			self.destroy_runner_sandbox(server, &sandbox, error).await?;
		}

		Ok(())
	}

	pub(crate) async fn destroy_runner_sandbox(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		error: tg::error::Data,
	) -> tg::Result<()> {
		tracing::warn!(sandbox = %id, reason = error.message.as_deref(), "finishing the runner sandbox");
		let now = server.clock.unix_timestamp();
		let length = server
			.index
			.try_get_sandbox_processes_count(id)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox in the index"))?;
		let processes = server
			.index
			.get_sandbox_processes(id, std::io::SeekFrom::Start(0), length)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox processes"))?;
		let indexed = server
			.index
			.try_get_processes_with_data(&processes)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the process records"))?;
		for (process, indexed) in processes.iter().zip(indexed) {
			let Some((indexed, data)) = indexed else {
				continue;
			};
			let process = process.clone();
			let mut data = data.ok_or_else(|| tg::error!(%process, "missing the process data"))?;
			let finish = data.status.is_started();
			if finish {
				if data.log.is_none() && (data.stdout.is_log() || data.stderr.is_log()) {
					match self
						.finish_expired_process_log(server, &process)
						.boxed()
						.await
					{
						Ok(log) => data.log = Some(log),
						Err(error) => {
							tracing::error!(error = %error.trace(), %process, "failed to recover the process log");
						},
					}
				}
				data.children.get_or_insert_default();
				data.cacheable = false;
				data.set_error(Some(tg::Either::Left(error.clone())));
				data.exit = Some(1);
				data.finished_at = Some(server.clock.unix_timestamp());
				data.status = tg::process::Status::Finished;
				let mut context = server.context.clone();
				context.principal = tg::Principal::Process(process.clone());
				let session = server.session(&context);
				let entry = tg::process::put::Arg {
					data: data.clone(),
					location: None,
				};
				let options = tangram_server_process_core::put::Options {
					command_sync: None,
					defer_index: false,
					location: None,
					outcome_sync: None,
					store_data: true,
				};
				session
					.server
					.components
					.process
					.put_process_local(&session, &process, entry, options)
					.boxed()
					.await
					.map_err(
						|source| tg::error!(!source, %process, "failed to store the finished process"),
					)?;
				let session = server.session(&server.context);
				if let Err(error) = session
					.server
					.components
					.process
					.expire_process_log_cache(&session, &process, &data)
					.await
				{
					tracing::error!(error = %error.trace(), %process, "failed to expire the process log cache");
				}
				server
					.components
					.process
					.notify_process_log(server, &process);
				session
					.server
					.components
					.process
					.spawn_process_finish_tasks(&session, &process);
			} else {
				// Remove the tokens before updating the index.
				let data = data.without_location_and_tokens_except_sync();

				server
					.index
					.batch(tangram_index::batch::Arg {
						items: std::iter::once(tangram_index::batch::Item::PutProcess(
							tangram_index::process::put::Arg {
								account: None,
								cached: false,
								captures: tangram_index::permission::capture::Captures::default(),
								children: None,
								command: Some(
									data.command
										.objects()
										.into_iter()
										.map(|object| object.node)
										.collect(),
								),
								command_id: data.command.command_id()?.into(),
								data: Some(data.clone()),
								error: None,
								id: process.clone(),
								location: None,
								log: None,
								metadata: indexed.metadata,
								options: tg::referent::Options::default(),
								output: None,
								parent: None,
								permissions: Vec::new(),
								principal: tg::Principal::Process(process.clone()),
								sandbox: None,
								storage: indexed.storage,
								time_to_touch: server.config.process.time_to_touch,
								touched_at: now,
							},
						))
						.collect(),
					})
					.await
					.and_then(std::convert::identity)
					.map_err(
						|source| tg::error!(!source, %process, "failed to update the process in the index"),
					)?;
			}
		}

		// Finalize the process history that survived the runner.
		let length = server
			.index
			.try_get_sandbox_processes_count(id)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox in the index"))?;
		let processes = server
			.index
			.try_get_sandbox_processes(id, std::io::SeekFrom::Start(0), length)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox processes in the index"))?;

		let mut indexed = server
			.index
			.try_get_sandbox(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox from the index"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox in the index"))?;
		let data = indexed
			.data
			.as_mut()
			.ok_or_else(|| tg::error!(%id, "missing the sandbox data"))?;
		data.data.error = Some(tg::Either::Left(error));
		data.data.status = tg::sandbox::Status::Destroyed;
		let account = if server.config.usage.enabled {
			indexed.account
		} else {
			None
		};
		server
			.index
			.batch(tangram_index::batch::Arg {
				items: vec![tangram_index::batch::Item::PutSandbox(
					tangram_index::sandbox::put::Arg {
						account,
						created_at: indexed.created_at,
						data: indexed.data,
						id: id.clone(),
						location: None,
						permissions: Vec::new(),
						principal: tg::Principal::Sandbox(id.clone()),
						processes: Some(processes),
						runner: indexed.runner,
						touched_at: now,
					},
				)],
			})
			.await
			.and_then(std::convert::identity)
			.map_err(
				|source| tg::error!(!source, %id, "failed to update the destroyed sandbox in the index"),
			)?;
		server
			.components
			.sandbox
			.spawn_publish_sandbox_status_task(server, id);

		Ok(())
	}

	async fn finish_expired_process_log(
		&self,
		server: &Server,
		process: &tg::process::Id,
	) -> tg::Result<tg::Referent<tg::blob::Id>> {
		self.end_expired_process_log(server, process).await?;
		let session = server.session(&server.context);
		let mut writer = tangram_server_log_common::Writer::new(&session);
		let mut position = 0;
		let streams = BTreeSet::from([
			tg::process::stdio::Stream::Stderr,
			tg::process::stdio::Stream::Stdout,
		]);
		loop {
			let arg = log::read::Arg {
				length: 64 * 1024,
				position,
				process: process.clone(),
				streams: streams.clone(),
			};
			let entries = server.cache.try_read_log(arg).await?;
			if entries.is_empty() {
				break;
			}
			for entry in entries {
				let bytes = bytes::Bytes::copy_from_slice(&entry.bytes);
				position = entry.position + u64::try_from(bytes.len()).unwrap();
				let chunk = tg::process::stdio::Chunk {
					bytes,
					combined_position: entry.position,
					stream: entry.stream,
					stream_position: entry.stream_position,
					timestamp: Some(entry.timestamp),
				};
				writer.write(&chunk).await?;
			}
		}
		let log = writer.end().await?;
		Ok(log)
	}

	async fn end_expired_process_log(
		&self,
		server: &Server,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		if server.cache.try_get_log_end(process).await?.is_some() {
			return Ok(());
		}

		// Recover the stored positions because the expired writer cannot send its end request.
		let length = async |streams| {
			let arg = log::length::Arg {
				process: process.clone(),
				streams,
			};
			let position = server
				.cache
				.try_get_log_length(arg)
				.await?
				.unwrap_or_default();
			Ok::<_, tg::Error>(position)
		};
		let (position, stderr_position, stdout_position) = tokio::try_join!(
			length(BTreeSet::from([
				tg::process::stdio::Stream::Stderr,
				tg::process::stdio::Stream::Stdout,
			])),
			length(BTreeSet::from([tg::process::stdio::Stream::Stderr])),
			length(BTreeSet::from([tg::process::stdio::Stream::Stdout])),
		)
		.map_err(|error| tg::error!(!error, %process, "failed to get the log positions"))?;
		let end = tg::process::log::End {
			position,
			stderr_position,
			stdout_position,
		};
		let arg = log::end::Arg {
			end,
			process: process.clone(),
		};

		// Persist the marker before publishing completion.
		server
			.cache
			.put_log_end(arg)
			.await
			.map_err(|error| tg::error!(!error, %process, "failed to store the log end"))?;

		Ok(())
	}
}
