use {
	super::{EnqueueSandboxRequestArg, Scheduler},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
};

impl Scheduler {
	/// Write the starting record and the key that associates the sandbox with this scheduler.
	/// Read the sandboxes that this scheduler has keys for, after pending indexing, so that every key written before the read is included.
	pub(super) async fn read_scheduler_sandbox_keys(&self) -> tg::Result<Vec<tg::sandbox::Id>> {
		self.index().await?;
		let sandboxes = self.server.index.get_scheduler_sandboxes(&self.id).await?;
		let keys = sandboxes
			.into_iter()
			.map(|sandbox| sandbox.id)
			.collect::<Vec<_>>();

		Ok(keys)
	}

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

		// Finish a process that never started, because no runner will report it.
		if !status.is_starting()
			&& let Some(process) = &request.process
		{
			self.server
				.components
				.process
				.finish_unstarted_process(&self.server, process, process_error.clone())
				.await?;
		}

		// Finish the other queued processes of a sandbox whose boot ended, which can never start.
		if status.is_stopped() || status.is_destroyed() {
			let result = self
				.server
				.components
				.process
				.finish_queued_sandbox_processes(
					&self.server,
					id,
					request.boot,
					Some(process_error),
				)
				.await;
			if let Err(error) = result {
				tracing::warn!(error = %error.trace(), %id, "failed to finish the queued processes of the ended sandbox");
			}
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
