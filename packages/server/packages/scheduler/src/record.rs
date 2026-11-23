use {
	super::{EnqueueSandboxRequestArg, Scheduler},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_index::prelude::*,
};

impl Scheduler {
	/// Await indexing, so that the index has applied every write that was acknowledged before.
	pub(super) async fn index(&self) -> tg::Result<()> {
		let session = self.server.session(&self.server.context);
		self.server
			.components
			.indexer
			.index(&session, tg::index::Arg::default())
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;

		Ok(())
	}

	/// Write the starting record and the key that associates the sandbox with this scheduler. The first write of a request also writes the permissions of its queued process.
	pub(super) async fn persist_sandbox(
		&self,
		request: &EnqueueSandboxRequestArg,
		first: bool,
	) -> tg::Result<()> {
		tangram_server_core::checkpoint!(self.server, "scheduler.sandbox.persist", sandbox = %request.sandbox)
			.await;
		let stored = EnqueueSandboxRequestArg {
			token: None,
			..request.clone()
		};
		let stored = serde_json::to_vec(&stored)
			.map_err(|error| tg::error!(!error, "failed to serialize the request"))?;
		self.put_sandbox_status(
			request,
			tg::sandbox::Status::Starting,
			None,
			Some(stored),
			first,
		)
		.await
	}

	/// Reserve the next boot of a stopped sandbox with one index write that the index rejects unless the sandbox is still stopped at the boot before it, which records the starting boot and the boot's request in this scheduler's queue, from which a restarted scheduler recovers the boot. Return whether the index applied the write.
	pub(super) async fn reserve_sandbox_boot(
		&self,
		request: &EnqueueSandboxRequestArg,
	) -> tg::Result<bool> {
		tangram_server_core::checkpoint!(self.server, "sandbox.start.reserve", sandbox = %request.sandbox)
			.await;

		// Create the starting record.
		let session = self.server.session(&self.server.context);
		let id = &request.sandbox;
		let now = self.server.clock.unix_timestamp();
		let data =
			tangram_server_scheduler_core::sandbox_data(request, tg::sandbox::Status::Starting);
		let location = tg::Location::Local(tg::location::Local {
			region: self.server.config.region.clone(),
		});
		let data = tg::sandbox::get::Output {
			data,
			location: Some(location.clone()),
			tokens: tg::authorization::Tokens::default(),
		};
		let account = self
			.server
			.components
			.usage
			.usage_account(&session, &request.owner)
			.await?;
		let stopped_boot = request
			.boot
			.checked_sub(1)
			.ok_or_else(|| tg::error!(%id, "expected the start of a later boot"))?;
		let put_arg = tangram_index::sandbox::put::Arg {
			account,
			created_at: request.created_at,
			data: Some(data),
			id: id.clone(),
			location: Some(location),
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			process: None,
			processes: None,
			registration: None,
			runner: request.runner.clone(),
			scheduler: Some(self.id.clone()),
			sequence: 0,
			stopped_boot: Some(stopped_boot),
			stopped_usage: None,
			touched_at: now,
		};

		// Create the entry in this scheduler's queue.
		let stored = EnqueueSandboxRequestArg {
			token: None,
			..request.clone()
		};
		let stored = serde_json::to_vec(&stored)
			.map_err(|error| tg::error!(!error, "failed to serialize the request"))?;
		let queue_arg = tangram_index::scheduler::sandbox::put::Arg {
			boot: request.boot,
			request: stored,
			sandbox: id.clone(),
			scheduler: self.id.clone(),
		};

		// Write both in one transaction, which the index rejects unless the sandbox is still stopped at the boot before the start.
		let items = vec![
			tangram_index::Item::PutSandbox(Box::new(put_arg.clone())),
			tangram_index::Item::PutSchedulerSandbox(Box::new(queue_arg)),
		];
		let arg = tangram_index::write::Arg { items };
		let result = self.server.index.write(arg).await;

		// Read the sandbox after a write that failed, because the write may have applied, and the scheduler must hold a boot whose reservation applied.
		let reserved = match result {
			Ok(result) => result.is_ok(),
			Err(source) => {
				let existing = loop {
					match self.server.index.try_get_sandbox(id).await {
						Ok(existing) => break existing,
						Err(error) => {
							tracing::error!(error = %error.trace(), %id, "failed to read the sandbox after its reservation failed");
							tokio::time::sleep(super::recovery::READ_RETRY_INTERVAL).await;
						},
					}
				};
				if !put_arg.is_applied(existing.as_ref()) {
					return Err(tg::error!(!source, %id, "failed to reserve the sandbox's boot"));
				}
				true
			},
		};
		if reserved {
			tangram_server_core::checkpoint!(self.server, "scheduler.sandbox.reserved", sandbox = %id)
				.await;
			self.server
				.components
				.sandbox
				.spawn_publish_sandbox_status_task(&self.server, id);
		}

		Ok(reserved)
	}

	/// Write the handoff of a boot to the runner that accepted it, which removes the boot from this scheduler's queue. The runner holds the boot from its yes, and its own first write records it as the sandbox's runner.
	pub(super) async fn write_sandbox_handoff(
		&self,
		request: &EnqueueSandboxRequestArg,
		runner: &tg::runner::Id,
	) -> tg::Result<()> {
		tangram_server_core::checkpoint!(self.server, "scheduler.sandbox.handoff", sandbox = %request.sandbox, %runner)
			.await;
		let id = &request.sandbox;
		let delete = tangram_index::scheduler::sandbox::delete::Arg {
			boot: request.boot,
			sandbox: id.clone(),
		};
		let arg = tangram_index::write::Arg {
			items: vec![tangram_index::Item::DeleteSchedulerSandbox(Box::new(
				delete,
			))],
		};
		self.server
			.components
			.indexer
			.write(&self.server, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to write the sandbox's handoff"))?;

		Ok(())
	}

	/// Record a failure after acceptance in the sandbox's error field. A new sandbox is destroyed, and a sandbox that was starting again is stopped.
	pub(super) async fn fail_sandbox(
		&self,
		request: &EnqueueSandboxRequestArg,
		error: tg::Either<tg::error::Data, tg::error::Id>,
	) -> tg::Result<()> {
		let status = failed_sandbox_status(request);
		self.put_sandbox_status(request, status, Some(error), None, false)
			.await
	}

	pub(super) async fn put_sandbox_status(
		&self,
		request: &EnqueueSandboxRequestArg,
		status: tg::sandbox::Status,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
		stored: Option<Vec<u8>>,
		permissions: bool,
	) -> tg::Result<()> {
		// Create the sandbox's data.
		let session = self.server.session(&self.server.context);
		let id = &request.sandbox;
		let now = self.server.clock.unix_timestamp();
		let mut data = tangram_server_scheduler_core::sandbox_data(request, status);
		let process_error = error.clone().unwrap_or_else(|| {
			tg::Either::Left(tg::error::Data {
				code: Some(tg::error::Code::Cancellation),
				message: Some("the sandbox was destroyed".to_owned()),
				..Default::default()
			})
		});
		data.error = error;
		if status.is_stopped() {
			data.expires_at = data.time_to_live.map(|time_to_live| {
				now.saturating_add(time_to_live.as_secs().try_into().unwrap_or(i64::MAX))
			});
			data.stopped_at = Some(now);
		}
		let account = self
			.server
			.components
			.usage
			.usage_account(&session, &request.owner)
			.await?;
		let location = tg::Location::Local(tg::location::Local {
			region: self.server.config.region.clone(),
		});
		let data = tg::sandbox::get::Output {
			data,
			location: Some(location.clone()),
			tokens: tg::authorization::Tokens::default(),
		};

		// Enqueue the write.
		let processes = (request.boot == 0 && status.is_destroyed()).then(Vec::new);
		let arg = tangram_index::sandbox::put::Arg {
			account,
			created_at: request.created_at,
			data: Some(data),
			id: id.clone(),
			location: Some(location),
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			process: None,
			processes,
			registration: None,
			runner: request.runner.clone(),
			scheduler: Some(self.id.clone()),
			sequence: 0,
			stopped_boot: None,
			stopped_usage: None,
			touched_at: now,
		};

		// Carry a queued process in the sandbox's item, so that the two are written together.
		let mut arg = arg;
		if status.is_starting()
			&& let Some(process) = &request.process
		{
			let process = self.server.components.process.create_queued_process_arg(
				&session,
				process,
				id,
				Some(request.boot),
				now,
				permissions,
				request.public,
			)?;
			arg.process = Some(Box::new(process));
		}
		let mut items = vec![tangram_index::Item::PutSandbox(Box::new(arg))];

		// Record the accepted request in the scheduler's queue with a starting sandbox, keep it while the scheduler destroys the sandbox, and remove it once the boot ends unplaced, in the same write as the sandbox's status.
		let boot = request.boot;
		if let Some(request) = stored {
			let arg = tangram_index::scheduler::sandbox::put::Arg {
				boot,
				request,
				sandbox: id.clone(),
				scheduler: self.id.clone(),
			};
			items.push(tangram_index::Item::PutSchedulerSandbox(Box::new(arg)));
		} else if !status.is_starting() && status != tg::sandbox::Status::Destroying {
			let arg = tangram_index::scheduler::sandbox::delete::Arg {
				boot,
				sandbox: id.clone(),
			};
			items.push(tangram_index::Item::DeleteSchedulerSandbox(Box::new(arg)));
		}
		let arg = tangram_index::write::Arg { items };
		self.server
			.components
			.indexer
			.write(&self.server, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put the sandbox in the index"))?;

		// Finish the boot's queued process, which this scheduler holds and which never started, from this scheduler's memory.
		if !status.is_starting()
			&& let Some(process) = &request.process
			&& let Some(process_id) = &process.id
		{
			self.server
				.components
				.process
				.finish_queued_process(
					&self.server,
					process_id,
					process.data.clone(),
					process_error,
					None,
				)
				.await?;
		}
		self.server
			.components
			.sandbox
			.spawn_publish_sandbox_status_task(&self.server, id);

		Ok(())
	}
}

/// Get the status that records a failure of a sandbox after its acceptance, which destroys a new sandbox and stops a sandbox that was starting again.
pub(super) fn failed_sandbox_status(request: &EnqueueSandboxRequestArg) -> tg::sandbox::Status {
	if request.boot == 0 {
		tg::sandbox::Status::Destroyed
	} else {
		tg::sandbox::Status::Stopped
	}
}
