use {
	super::{Operation, Scheduler, SchedulerState},
	futures::FutureExt as _,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_server_core::{
		Server,
		control::accept::{Accept, Path},
	},
};

/// A spawn into an existing sandbox, which this scheduler holds from its write of the process's queued record until the sandbox's runner accepts the process, and which this scheduler's saved queue keeps, so that a restarted scheduler offers it again.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(super) struct Spawn {
	/// The spawn, which carries the boot that it is for.
	pub(super) arg: tg::sandbox::control::SpawnProcessServerRequestArg,

	/// The ID of the spawn's request, with which this scheduler answers the spawn's sender.
	pub(super) id: String,

	pub(super) sandbox: tg::sandbox::Id,
}

impl SchedulerState {
	/// Hold a spawn into a sandbox that a runner holds, and offer the process to the runner with the boot, in an operation.
	pub(super) fn spawn_process(&mut self, scheduler: &Scheduler, spawn: Spawn, record: bool) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler.spawn_process(spawn, record).boxed().await;
				Operation::Finished {
					context: "failed to send the spawn to the sandbox's runner",
					result,
				}
			}
			.boxed(),
		);
	}

	/// Offer the processes that this scheduler's saved queue holds again after a restart, and remove from the queue those whose runner already took them or that finished. Remember that this scheduler offered each process into its boot, so that the runner's reports of the boot may finish it.
	pub(super) fn recover_spawns(&mut self, scheduler: &Scheduler, spawns: Vec<Spawn>) {
		for spawn in spawns {
			if let Some(process) = &spawn.arg.process.id
				&& let Some(entry) = self.sandboxes.entries.get_mut(&spawn.sandbox)
				&& Some(entry.boot) == spawn.arg.boot
			{
				entry.processes.insert(process.clone());
			}
			let scheduler = scheduler.clone();
			self.operations.push(
				async move {
					let result = scheduler.recover_spawn(spawn).boxed().await;
					Operation::Finished {
						context: "failed to recover the spawn",
						result,
					}
				}
				.boxed(),
			);
		}
	}
}

impl Scheduler {
	/// Hold a spawn into a sandbox that a runner holds: write the process's queued record with its permissions and its entry in this scheduler's queue, and offer the process to the runner with the boot concurrently, and answer the spawn's sender as soon as either the write or the runner's yes succeeds, so that the spawn does not wait for the write to persist. The runner's yes hands the process off, and every other outcome leaves the process to the runner if it accepted the process and finishes it as failed otherwise.
	pub(super) async fn spawn_process(&self, spawn: Spawn, record: bool) -> tg::Result<()> {
		let held = spawn.arg.process.id.is_some();

		// Write the record and offer the process to the runner concurrently, and answer the spawn's sender once either succeeds, or once both fail.
		let record = record && held;
		let mut record_future = pin!(self.record_spawn(&spawn));
		let mut offer_future = pin!(self.offer_spawn(&spawn));
		let mut accept = Accept::default();
		if !record {
			accept.finish(Path::Queued, Err(tg::error!("the spawn is not recorded")));
		}
		let mut recorded = !record;
		let mut offered = None;
		while !recorded || offered.is_none() {
			let decision = tokio::select! {
				result = &mut record_future, if !recorded => {
					recorded = true;
					accept.finish(Path::Queued, result).map(|result| {
						result.and_then(|()| {
							let id = spawn
								.arg
								.process
								.id
								.clone()
								.ok_or_else(|| tg::error!("expected the process's ID"))?;
							let output = tg::sandbox::control::SpawnProcessClientResponseOutput {
								lease: spawn.arg.process.lease,
								process: tg::Referent::with_node(id),
							};
							Ok(tg::sandbox::control::ClientResponseOutput::SpawnProcess(output))
						})
					})
				},
				result = &mut offer_future, if offered.is_none() => {
					let decision = accept
						.finish(Path::Handled, result.as_ref().map(|_| ()).map_err(Clone::clone))
						.map(|decision| decision.and_then(|()| result.clone()));
					offered = Some(result);
					decision
				},
			};
			if let Some(result) = decision {
				self.answer_spawn(&spawn, result).await?;
			}
		}
		let offered = offered.unwrap();

		// Hand the process off on the runner's yes, and otherwise finish it unless the runner accepted it.
		if held {
			match &offered {
				Ok(_) => self.accept_process(&spawn).await,
				Err(error) => self.finish_unaccepted_spawn(&spawn, error).await,
			}
		}

		Ok(())
	}

	/// Answer the spawn's sender.
	async fn answer_spawn(
		&self,
		spawn: &Spawn,
		result: tg::Result<tg::sandbox::control::ClientResponseOutput>,
	) -> tg::Result<()> {
		let response = match result {
			Ok(output) => tg::sandbox::control::ClientResponse {
				error: None,
				id: spawn.id.clone(),
				output: Some(output),
				retry: None,
			},
			Err(error) => tg::sandbox::control::ClientResponse {
				error: Some(
					error
						.to_data_or_id()
						.left()
						.unwrap_or_else(|| tg::error::Data {
							message: Some(error.to_string()),
							..Default::default()
						}),
				),
				id: spawn.id.clone(),
				output: None,
				retry: None,
			},
		};
		self.server
			.components
			.sandbox
			.publish_sandbox_holder_response(
				&self.server,
				&spawn.sandbox,
				tangram_server_core::sandbox::Holder::Scheduler,
				response,
			)
			.await?;

		Ok(())
	}

	/// Write the process's queued record with its permissions, and its entry in this scheduler's queue, in one write.
	async fn record_spawn(&self, spawn: &Spawn) -> tg::Result<()> {
		tangram_server_core::checkpoint!(self.server, "scheduler.process.record", sandbox = %spawn.sandbox).await;
		let session = self.server.session(&self.server.context);
		let process = &spawn.arg.process;
		let id = process
			.id
			.clone()
			.ok_or_else(|| tg::error!("expected the process's ID"))?;
		let now = self.server.clock.unix_timestamp();
		let record = self.server.components.process.create_queued_process_arg(
			&session,
			process,
			&spawn.sandbox,
			spawn.arg.boot,
			now,
			true,
			spawn.arg.public,
		)?;

		// Create the entry, which stores the spawn without the process's token, which a restarted scheduler creates again.
		let mut stored = spawn.clone();
		stored.arg.process.token = None;
		let request = serde_json::to_vec(&stored)
			.map_err(|error| tg::error!(!error, "failed to serialize the spawn"))?;
		let entry = tangram_index::scheduler::process::put::Arg {
			process: id,
			request,
			sandbox: spawn.sandbox.clone(),
			scheduler: self.id.clone(),
		};

		// Write both.
		let arg = tangram_index::write::Arg {
			items: vec![
				tangram_index::Item::PutProcess(Box::new(record)),
				tangram_index::Item::PutSchedulerProcess(Box::new(entry)),
			],
		};
		self.server
			.components
			.indexer
			.write(&self.server, arg)
			.await
			.map_err(
				|error| tg::error!(!error, sandbox = %spawn.sandbox, "failed to write the process"),
			)?;

		Ok(())
	}

	/// Offer a process to the runner with the boot by sending the spawn to the sandbox's holder, and send it again on each status wakeup, after each timeout, and after each failure to send it, with no limit, because the holder helper's resends spend no retries, until the runner answers or the boot ends, such as when its runner is declared dead. Return the output of the runner's yes, and an error for every other outcome.
	async fn offer_spawn(
		&self,
		spawn: &Spawn,
	) -> tg::Result<tg::sandbox::control::ClientResponseOutput> {
		let component = &self.server.components.sandbox;
		let session = self.server.session(&self.server.context);
		let sandbox = &spawn.sandbox;
		let boot = spawn.arg.boot.unwrap_or_default();
		let mut wakeups = component
			.create_sandbox_status_wakeup_stream(&session, sandbox, None, None)
			.await?;
		let request = tg::sandbox::control::ServerRequestArg::SpawnProcess(spawn.arg.clone());
		tangram_server_core::checkpoint!(self.server, "scheduler.process.spawn", %sandbox).await;
		let reply = loop {
			let retry = self.server.config.control.request_retry.clone().into();
			let timeout = self.server.config.control.request_timeout;
			let options = tangram_server_core::control::Options { retry, timeout };
			let holder_arg = tangram_server_core::sandbox::HolderArg {
				checkpoint: None,
				deadline: self.server.control_read_deadline(),
				exhaustive: true,
				id: format!("{}.{boot}", spawn.id),
				mode: tangram_server_core::holder::Mode::Write,
				options,
				request: request.clone(),
				wakeups: &mut wakeups,
			};
			match component
				.request_sandbox_holder(&session, sandbox, holder_arg)
				.await
			{
				Ok(output) => break output.and_then(|output| output.reply),
				Err(error) => {
					tracing::warn!(error = %error.trace(), %sandbox, "failed to send the spawn to the sandbox's runner");
					tokio::time::sleep(super::recovery::READ_RETRY_INTERVAL).await;
				},
			}
		};
		match reply {
			Some(Ok(output @ tg::sandbox::control::ClientResponseOutput::SpawnProcess(_))) => {
				Ok(output)
			},
			Some(Ok(_)) => Err(tg::error!(%sandbox, "expected a spawn process response")),
			Some(Err(error)) => Err(error),
			None => Err(tg::error!(
				code = tg::error::Code::Cancellation,
				%sandbox,
				"the sandbox is not started"
			)),
		}
	}

	/// Record a runner's yes to a process, which is this scheduler's handoff of the process: remove the process from this scheduler's queue. Every yes to a process goes through this function. Write the handoff again until it applies, because the process stays in this scheduler's saved queue until the handoff removes it.
	async fn accept_process(&self, spawn: &Spawn) {
		loop {
			let result = self.write_process_handoff(spawn).await;
			let Err(error) = result else {
				break;
			};
			tracing::error!(error = %error.trace(), sandbox = %spawn.sandbox, "failed to record the handoff of the process");
			tokio::time::sleep(super::recovery::READ_RETRY_INTERVAL).await;
		}
	}

	/// Finish a process whose offer ended without the runner's yes, such as when its boot ended before its runner received it, as failed, and remove it from this scheduler's queue. This is the exception to the rule that a non-holder changes nothing: the scheduler that spawned the process into an existing sandbox ends it when its offer gets no yes.
	async fn finish_unaccepted_spawn(&self, spawn: &Spawn, error: &tg::Error) {
		finish_spawn(&self.server, &self.id, spawn, error).await;
	}

	/// Remove a process from this scheduler's queue.
	async fn write_process_handoff(&self, spawn: &Spawn) -> tg::Result<()> {
		delete_spawn(&self.server, &self.id, spawn).await
	}

	/// Read the saved spawns from this scheduler's queue, with the processes' tokens created again.
	pub(super) async fn get_saved_spawns(&self) -> tg::Result<Vec<Spawn>> {
		get_spawns(&self.server, &self.id).await
	}

	/// Offer a saved spawn again if its process is still queued, and otherwise remove it from the queue, because its runner took it or it finished.
	async fn recover_spawn(&self, spawn: Spawn) -> tg::Result<()> {
		let id = spawn
			.arg
			.process
			.id
			.clone()
			.ok_or_else(|| tg::error!("expected the process's ID"))?;
		let indexed = self
			.server
			.index
			.try_get_processes_with_data(std::slice::from_ref(&id))
			.await
			.map_err(|error| tg::error!(!error, process = %id, "failed to get the process"))?
			.pop()
			.flatten()
			.and_then(|(_, data)| data);
		if indexed.is_some_and(|data| data.status.is_queued()) {
			self.spawn_process(spawn, false).boxed().await?;
		} else {
			self.write_process_handoff(&spawn).await?;
		}

		Ok(())
	}
}

/// Finish the saved spawns of a single server's previous run as failed, because its runner's sandboxes ended with the run.
pub(super) async fn end_spawns(
	server: &Server,
	scheduler: &tg::scheduler::Id,
	error: &tg::Error,
) -> tg::Result<()> {
	let spawns = get_spawns(server, scheduler).await?;
	for spawn in spawns {
		finish_spawn(server, scheduler, &spawn, error).await;
	}

	Ok(())
}

/// Read the saved spawns from a scheduler's queue, with the processes' tokens created again.
async fn get_spawns(server: &Server, scheduler: &tg::scheduler::Id) -> tg::Result<Vec<Spawn>> {
	let entries = server
		.index
		.get_scheduler_processes(scheduler)
		.await
		.map_err(|error| tg::error!(!error, "failed to get the scheduler's processes"))?;
	let mut spawns = Vec::with_capacity(entries.len());
	for entry in entries {
		let mut spawn = serde_json::from_slice::<Spawn>(&entry.request).map_err(
			|error| tg::error!(!error, process = %entry.id, "failed to deserialize the spawn"),
		)?;
		let token = server
			.components
			.authentication
			.create_process_authentication_token(server, entry.id)?;
		spawn.arg.process.token = Some(token);
		spawns.push(spawn);
	}

	Ok(spawns)
}

/// Finish a process that a scheduler holds as failed with the error, and remove it from the scheduler's queue.
async fn finish_spawn(
	server: &Server,
	scheduler: &tg::scheduler::Id,
	spawn: &Spawn,
	error: &tg::Error,
) {
	let process = &spawn.arg.process;
	let Some(id) = &process.id else {
		return;
	};
	let data = process
		.data
		.clone()
		.without_location_and_tokens_except_sync();
	let result = server
		.components
		.process
		.finish_queued_process(server, id, data, error.to_data_or_id(), 0)
		.await;
	if let Err(error) = result {
		tracing::error!(error = %error.trace(), process = %id, "failed to finish the process");
		return;
	}
	if let Err(error) = delete_spawn(server, scheduler, spawn).await {
		tracing::error!(error = %error.trace(), process = %id, "failed to remove the process from the queue");
	}
}

/// Remove a process from a scheduler's queue.
async fn delete_spawn(
	server: &Server,
	scheduler: &tg::scheduler::Id,
	spawn: &Spawn,
) -> tg::Result<()> {
	let process = spawn
		.arg
		.process
		.id
		.clone()
		.ok_or_else(|| tg::error!("expected the process's ID"))?;
	let deleted_at = server.clock.unix_timestamp();
	let delete = tangram_index::scheduler::process::delete::Arg {
		deleted_at,
		process,
		sandbox: spawn.sandbox.clone(),
		scheduler: scheduler.clone(),
	};
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::DeleteSchedulerProcess(Box::new(
			delete,
		))],
	};
	server
		.components
		.indexer
		.write(server, arg)
		.await
		.map_err(
			|error| tg::error!(!error, sandbox = %spawn.sandbox, "failed to remove the process from the queue"),
		)?;

	Ok(())
}
