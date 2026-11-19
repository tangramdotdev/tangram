use {
	super::{EnqueueSandboxRequestArg, Scheduler},
	futures::FutureExt as _,
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
			stopped_boot: None,
			stopped_usage: None,
			touched_at: now,
		};

		// Carry a queued process in the sandbox's item, so that the two are written together.
		let mut arg = arg;
		if status.is_starting()
			&& let Some(process) = &request.process
		{
			let process = self.queued_process_arg(process, id, now, permissions)?;
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
			self.finish_unstarted_process(process, process_error)
				.await?;
		}
		self.server
			.components
			.sandbox
			.spawn_publish_sandbox_status_task(&self.server, id);

		Ok(())
	}

	/// Create the queued record of a process, which is visible until its runner connects, with the permissions of its creator if requested.
	pub(super) fn queued_process_arg(
		&self,
		process: &tg::runner::control::Process,
		sandbox: &tg::sandbox::Id,
		now: i64,
		permissions: bool,
	) -> tg::Result<tangram_index::process::put::Arg> {
		let id = process
			.id
			.clone()
			.ok_or_else(|| tg::error!("expected the scheduled process to have an id"))?;

		// Create the creator permission and the command captures from the data with its tokens, so that the creator can access the process as soon as it is visible.
		let (captures, permissions) = if permissions {
			self.queued_process_permissions(&id, &process.data)?
		} else {
			(
				tangram_index::permission::capture::Captures::default(),
				Vec::new(),
			)
		};

		// Create the record.
		let data = process
			.data
			.clone()
			.without_location_and_tokens_except_sync();
		let location = tg::Location::Local(tg::location::Local {
			region: self.server.config.region.clone(),
		});
		let arg = tangram_index::process::put::Arg {
			account: None,
			cached: false,
			captures,
			children: None,
			command: Some(
				data.command
					.objects()
					.into_iter()
					.map(|object| object.node)
					.collect(),
			),
			command_id: data.command.command_id()?.into(),
			data: Some(data),
			error: None,
			id: id.clone(),
			location: Some(location),
			log: None,
			metadata: tg::process::Metadata::default(),
			options: tg::referent::Options::default(),
			output: None,
			parent: None,
			permissions,
			principal: tg::Principal::Process(id),
			registration: None,
			sandbox: Some(sandbox.clone()),
			storage: tg::process::storage::Set::NODE,
			time_to_touch: self.server.config.process.time_to_touch,
			touched_at: now,
		};

		Ok(arg)
	}

	/// Create the permission that lets the creator of a queued process access it, and the captures that give the process the creator's access to its command.
	fn queued_process_permissions(
		&self,
		id: &tg::process::Id,
		data: &tg::process::Data,
	) -> tg::Result<(
		tangram_index::permission::capture::Captures,
		Vec<tangram_index::permission::put::Arg>,
	)> {
		let config = &self.server.config.process;
		let permissions = tangram_server_process_core::permission::creator_permission_arg(
			&data.creator,
			id,
			data.created_at,
			config.permission_time_to_live,
			config.permission_time_to_touch,
		)?
		.into_iter()
		.collect();
		let session = self.server.session(&self.server.context);
		let roots = data
			.command
			.objects()
			.into_iter()
			.map(|root| root.map(Into::into))
			.collect();
		let captures = self.server.components.authorization.create_captures(
			&session,
			id.clone().into(),
			None,
			roots,
			data.creator.clone(),
			data.created_at,
		)?;
		let output = (captures, permissions);

		Ok(output)
	}

	/// Record the failure of a process whose sandbox ended before the process started.
	pub(super) async fn finish_unstarted_process(
		&self,
		process: &tg::runner::control::Process,
		error: tg::Either<tg::error::Data, tg::error::Id>,
	) -> tg::Result<()> {
		let Some(id) = process.id.clone() else {
			return Ok(());
		};
		let mut data = process.data.clone();
		data.cacheable = false;
		data.children.get_or_insert_default();
		data.exit = Some(1);
		data.finished_at = Some(self.server.clock.unix_timestamp());
		data.set_error(Some(error.map_right(tg::Referent::with_node)));
		data.started_at = None;
		data.status = tg::process::Status::Finished;
		let mut context = self.server.context.clone();
		context.principal = tg::Principal::Process(id.clone());
		let session = self.server.session(&context);
		let entry = tg::process::put::Arg {
			data,
			location: None,
			public: false,
		};
		let options = tangram_server_process_core::put::Options {
			command_sync: None,
			defer_index: false,
			location: None,
			outcome_sync: None,
			registration: None,
			store_data: true,
		};
		self.server
			.components
			.process
			.put_process_local(&session, &id, entry, options)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, process = %id, "failed to store the process"))?;
		let session = self.server.session(&self.server.context);
		self.server
			.components
			.process
			.spawn_process_finish_tasks(&session, &id);

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
