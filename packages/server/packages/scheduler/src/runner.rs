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
}

pub(super) struct Runner {
	pub borrowable: HashSet<tg::sandbox::Id, tg::id::BuildHasher>,
	pub capacity: tg::runner::control::Capacity,
	pub committed: tg::runner::Capacity,
	pub heartbeat_at: tangram_server_core::clock::Instant,
	pub heartbeat_index: u64,
	pub host: String,
	pub owner: Option<tg::Id>,
	pub ready: bool,
	pub registration_index: u64,
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
			.map(|(id, runner)| (id.clone(), runner.registration_index))
			.collect()
	}
}

impl SchedulerState {
	pub(super) fn handle_add_runner_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: AddRunnerRequestArg,
	) {
		if self
			.runners
			.entries
			.get(&request.runner)
			.is_some_and(|runner| request.registration_index < runner.registration_index)
		{
			let error = tg::error!(
				code = tg::error::Code::Cancellation,
				"the runner registration was superseded"
			);
			let response = super::request::Response::new(id, Err(error));
			scheduler.send_response(self, response);
			return;
		}
		let registration_index = request.registration_index;
		tracing::info!(runner = %request.runner, registration_index, "registering the runner");
		self.handle_recovery_report(
			scheduler,
			&request.runner,
			registration_index,
			&request.sandboxes,
		);
		let completions = self.remove_runner(scheduler, &request.runner);
		scheduler.send_dequeue_sandbox_completions(self, completions);
		self.reconcile_placed_sandboxes(
			scheduler,
			&request.runner,
			registration_index,
			&request.sandboxes,
		);
		let runner = Runner {
			borrowable: HashSet::default(),
			capacity: request.capacity,
			committed: tg::runner::Capacity::default(),
			heartbeat_at: scheduler.server.clock.instant(),
			heartbeat_index: 0,
			host: request.host.clone(),
			owner: None,
			ready: false,
			registration_index: request.registration_index,
			requests: 0,
			reservations: HashMap::default(),
			reserved: tg::runner::Capacity::default(),
		};
		self.runners.entries.insert(request.runner.clone(), runner);

		// Track the boots that the runner reports, which a restarted scheduler rebuilds its tracking from.
		for sandbox in &request.sandboxes {
			self.track_scheduled_sandbox(
				sandbox.id.clone(),
				sandbox.boot,
				&request.runner,
				registration_index,
			);
		}
		self.queue.wake();

		let scheduler = scheduler.clone();
		let runner = request.runner.clone();
		self.operations.push(
			async move {
				let result = scheduler.add_runner(request).await;
				Operation::AddRunner {
					registration_index,
					id,
					result,
					runner,
				}
			}
			.boxed(),
		);
	}

	pub(super) fn handle_destroy_runner_sandbox_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: super::DestroyRunnerSandboxRequestArg,
	) {
		// Fail if the runner is not connected, because only the runner can destroy the sandbox.
		let Some(registration_index) = self
			.runners
			.entries
			.get(&request.runner)
			.map(|runner| runner.registration_index)
		else {
			let error = tg::error!(sandbox = %request.sandbox, runner = %request.runner, "the sandbox's runner is unavailable");
			let response = super::request::Response::new(id, Err(error));
			scheduler.send_response(self, response);
			return;
		};

		// Ask the runner to destroy the sandbox.
		let session = scheduler.server.session(&scheduler.server.context);
		let scheduler_id = scheduler.id.clone();
		let retry = scheduler.server.config.control.request_retry.clone().into();
		let timeout = scheduler.server.config.control.request_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		self.operations.push(
			async move {
				let arg = tg::runner::control::ServerRequestArg::DestroySandbox(
					tg::runner::control::DestroySandboxServerRequestArg {
						error: request.error.clone(),
						sandbox: request.sandbox.clone(),
					},
				);
				let result = session
					.server
					.components
					.runner
					.send_runner_control_request(
						&session,
						&request.runner,
						&scheduler_id,
						registration_index,
						arg,
						options,
					)
					.await
					.and_then(std::convert::identity)
					.and_then(|output| {
						output
							.try_unwrap_destroy_sandbox()
							.map_err(|_| tg::error!("expected a destroy sandbox response"))
							.map(|output| output.destroyed)
					});
				super::Operation::DestroyRunnerSandbox { id, result }
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
			.is_some_and(|runner| runner.registration_index == request.registration_index);
		if !current {
			if let Some(id) = id {
				let output = RemoveRunnerResponseOutput {
					runner: request.runner,
				};
				let response = super::request::Response::new(
					id,
					Ok(super::ResponseOutput::RemoveRunner(output)),
				);
				scheduler.send_response(self, response);
			}
			return;
		}
		tracing::info!(runner = %request.runner, registration_index = request.registration_index, "removing the runner");
		let completions = self.remove_runner(scheduler, &request.runner);
		scheduler.send_dequeue_sandbox_completions(self, completions);

		// Requeue the sandboxes that the runner held but that never connected.
		self.handle_lost_runner(scheduler, &request.runner);

		// Discard the starts pinned to the runner, so that a boot that the runner's sandboxes are marked stopped for never starts later.
		for discarded in self.discard_pinned_sandboxes(scheduler, &request.runner) {
			scheduler.publish_sandbox_discarded(self, discarded);
		}
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

		tangram_server_core::checkpoint!(self.server, "scheduler.runner.register", runner = %request.runner, registration_index = request.registration_index).await;
		self.server
			.components
			.runner
			.set_runner_scheduler(
				&session,
				&request.runner,
				&self.id,
				request.registration_index,
			)
			.await?;
		tangram_server_core::checkpoint!(self.server, "scheduler.runner.added", runner = %request.runner).await;
		let output = AddRunnerResponseOutput {
			runner: request.runner,
			scheduler: self.id.clone(),
		};

		Ok((output, owner))
	}

	pub(super) async fn remove_runner(
		&self,
		request: RemoveRunnerRequestArg,
	) -> tg::Result<RemoveRunnerResponseOutput> {
		// Stop the runner's sandboxes only while this registration still owns the runner.
		let registration = tangram_index::runner::Registration {
			index: request.registration_index,
			runner: request.runner.clone(),
			scheduler: self.id.clone(),
		};
		let result = self
			.component
			.handle_expired_runner(&self.server, &registration)
			.boxed()
			.await;
		if let Err(error) = result {
			let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
			let mut superseded = false;
			while let Some(error) = source {
				if error.downcast_ref::<tg::Error>().is_some_and(|error| {
					matches!(error.code(), Some(tg::error::Code::Cancellation))
				}) {
					superseded = true;
					break;
				}
				source = error.source();
			}
			if !superseded {
				return Err(error);
			}
		}

		tangram_server_core::checkpoint!(self.server, "scheduler.runner.removed", runner = %request.runner).await;

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
		registration: &tangram_index::runner::Registration,
	) -> tg::Result<()> {
		let runner = &registration.runner;
		let sandboxes =
			server.index.get_runner_sandboxes(runner).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"),
			)?;
		tracing::warn!(%runner, sandboxes = sandboxes.len(), "stopping the expired runner sandboxes");
		tangram_server_core::checkpoint!(server, "scheduler.runner.expired", %runner).await;
		let indexed =
			server.index.try_get_sandboxes(&sandboxes).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"),
			)?;
		for (sandbox, indexed) in std::iter::zip(sandboxes, indexed) {
			// Destroy a sandbox with a zero time to live, because its processes are lost, and stop the others.
			let error = tg::error::Data {
				code: Some(tg::error::Code::Internal),
				message: Some("heartbeat expired".to_owned()),
				..Default::default()
			};
			let time_to_live = indexed
				.and_then(|indexed| indexed.data)
				.and_then(|data| data.data.time_to_live);
			let status = if time_to_live.is_some_and(|time_to_live| time_to_live.is_zero()) {
				tg::sandbox::Status::Destroyed
			} else {
				tg::sandbox::Status::Stopped
			};
			self.finish_runner_sandbox(server, &sandbox, error, status, Some(registration))
				.boxed()
				.await?;
		}

		Ok(())
	}

	pub(crate) async fn destroy_runner_sandbox(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		error: tg::error::Data,
	) -> tg::Result<()> {
		self.finish_runner_sandbox(server, id, error, tg::sandbox::Status::Destroyed, None)
			.boxed()
			.await
	}

	pub(crate) async fn stop_runner_sandbox(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		error: tg::error::Data,
	) -> tg::Result<()> {
		self.finish_runner_sandbox(server, id, error, tg::sandbox::Status::Stopped, None)
			.boxed()
			.await
	}

	async fn finish_runner_sandbox(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		error: tg::error::Data,
		status: tg::sandbox::Status,
		registration: Option<&tangram_index::runner::Registration>,
	) -> tg::Result<()> {
		// Leave a sandbox that is already stopped or destroyed alone.
		let stopped_or_destroyed = server
			.index
			.try_get_sandbox(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox from the index"))?
			.and_then(|sandbox| sandbox.data)
			.is_some_and(|data| match status {
				tg::sandbox::Status::Destroyed => data.data.status.is_destroyed(),
				tg::sandbox::Status::Destroying
				| tg::sandbox::Status::Started
				| tg::sandbox::Status::Starting
				| tg::sandbox::Status::Stopped
				| tg::sandbox::Status::Stopping => data.data.status.is_stopped_or_destroyed(),
			});
		if stopped_or_destroyed {
			return Ok(());
		}
		tracing::warn!(sandbox = %id, reason = error.message.as_deref(), %status, "finishing the runner sandbox");
		let now = server.clock.unix_timestamp();
		let length = server
			.index
			.try_get_sandbox_processes_count(id)
			.await?
			.unwrap_or_default();
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
				let recover_log =
					data.log.is_none() && (data.stdout.is_log() || data.stderr.is_log());
				if recover_log {
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
					registration: registration.cloned(),
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

				// Finalize the cache only after the fenced process write succeeds.
				if recover_log {
					self.end_expired_process_log(server, &process).await?;
				}
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

				let arg = tangram_index::write::Arg {
					items: std::iter::once(tangram_index::Item::PutProcess(Box::new(
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
							registration: registration.cloned(),
							sandbox: None,
							storage: indexed.storage,
							time_to_touch: server.config.process.time_to_touch,
							touched_at: now,
						},
					)))
					.collect(),
				};
				server
					.index
					.write(arg)
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
			.unwrap_or_default();
		let processes = server
			.index
			.try_get_sandbox_processes(id, std::io::SeekFrom::Start(0), length)
			.await?
			.unwrap_or_default();

		let mut indexed = server
			.index
			.try_get_sandbox(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox from the index"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox in the index"))?;

		// Set the sandbox's error and status, and clear its usage, which the index already records for each boot, so that the sum of every boot is not recorded again as this boot's usage.
		let data = indexed
			.data
			.as_mut()
			.ok_or_else(|| tg::error!(%id, "missing the sandbox data"))?;
		data.data.error = Some(tg::Either::Left(error));
		data.data.status = status;
		data.data.usage = None;

		// Record a stop as of now, so that the stopped sandbox is charged and expires from now rather than from an earlier stop.
		if status.is_stopped() {
			let stopped_at = server.clock.unix_timestamp();
			data.data.expires_at = data.data.time_to_live.map(|time_to_live| {
				stopped_at.saturating_add(time_to_live.as_secs().try_into().unwrap_or(i64::MAX))
			});
			data.data.stopped_at = Some(stopped_at);
		}
		let account = if server.config.usage.enabled {
			indexed.account
		} else {
			None
		};
		let arg = tangram_index::write::Arg {
			items: vec![tangram_index::Item::PutSandbox(Box::new(
				tangram_index::sandbox::put::Arg {
					account,
					created_at: indexed.created_at,
					data: indexed.data,
					id: id.clone(),
					location: None,
					permissions: Vec::new(),
					principal: tg::Principal::Sandbox(id.clone()),
					process: None,
					processes: Some(processes),
					registration: registration.cloned(),
					runner: indexed.runner,
					scheduled: None,
					stopped_usage: None,
					touched_at: now,
				},
			))],
		};
		server
			.index
			.write(arg)
			.await
			.and_then(std::convert::identity)
			.map_err(
				|source| tg::error!(!source, %id, "failed to update the finished sandbox in the index"),
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
