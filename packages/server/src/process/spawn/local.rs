use {
	super::child::AddProcessChildArg,
	crate::Session,
	futures::{FutureExt as _, future},
	std::pin::pin,
	tangram_client::prelude::*,
};

#[derive(derive_more::Debug)]
pub(super) struct Output {
	#[debug(ignore)]
	pub allocation: Option<crate::runner::capacity::Allocation>,
	pub cached: bool,
	pub data: tg::process::Data,
	pub id: tg::process::Id,
	pub lease: Option<String>,
	pub parent: Option<tg::process::Id>,
	pub parent_sandbox: Option<tg::sandbox::Id>,
	pub process_token: Option<String>,
	pub sandbox_arg: Option<tg::sandbox::create::Arg>,
	pub sandbox_token: Option<String>,
	pub scheduler: Option<tg::scheduler::Id>,
	pub tokens: Vec<tg::authorization::Token>,
}

impl Output {
	pub fn try_outcome(&self) -> tg::Result<Option<tg::process::outcome::Data>> {
		if !self.data.status.is_finished() {
			return Ok(None);
		}
		let error = self.data.error.clone();
		let exit = self
			.data
			.exit
			.ok_or_else(|| tg::error!(process = %self.id, "expected the exit to be set"))?;
		Ok(Some(tg::process::outcome::Data {
			error,
			exit,
			output: self.data.output.clone(),
		}))
	}
}

impl Session {
	pub(crate) fn try_acquire_sandbox_capacity(
		&self,
		parent: Option<&tg::sandbox::Id>,
		requested: tg::runner::Capacity,
	) -> Option<crate::runner::capacity::Allocation> {
		if let Some(parent) = parent
			&& let Some(sandbox) = self.server.runner.state().sandboxes().get_by_id(parent)
			&& let Some(allocation) = sandbox.allocation.clone()
			&& let Ok(parent) = allocation.try_lock_owned()
			&& let Some(allocation) =
				crate::runner::capacity::Allocation::try_borrow(parent, requested)
		{
			return Some(allocation);
		}
		self.server.runner.state().capacity().try_acquire(requested)
	}

	pub(crate) fn try_acquire_scheduled_sandbox_capacity(
		&self,
		borrowed: bool,
		parent: Option<&tg::sandbox::Id>,
		requested: tg::runner::Capacity,
	) -> Option<crate::runner::capacity::Allocation> {
		if borrowed {
			let parent = parent?;
			return self
				.server
				.runner
				.state()
				.reservations()
				.try_acquire(parent, requested)
				.or_else(|| self.try_acquire_parent_sandbox_capacity(parent, requested));
		}

		self.try_acquire_sandbox_capacity(parent, requested)
	}

	fn try_acquire_parent_sandbox_capacity(
		&self,
		parent: &tg::sandbox::Id,
		requested: tg::runner::Capacity,
	) -> Option<crate::runner::capacity::Allocation> {
		let sandbox = self.server.runner.state().sandboxes().get_by_id(parent)?;
		let allocation = sandbox.allocation.clone()?;
		let parent = allocation.try_lock_owned().ok()?;

		crate::runner::capacity::Allocation::try_borrow(parent, requested)
	}

	pub(super) async fn spawn_process_notify_borrowable_capacity(
		&self,
		parent: &tg::process::Id,
		parent_sandbox: &tg::sandbox::Id,
		requested: tg::runner::Capacity,
	) -> tg::Result<()> {
		let sandbox = self
			.server
			.runner
			.state()
			.sandboxes()
			.get_by_id(parent_sandbox)
			.ok_or_else(|| tg::error!(%parent_sandbox, "failed to find the parent sandbox"))?;
		let allocation = sandbox.allocation.clone().ok_or_else(
			|| tg::error!(%parent_sandbox, "failed to find the parent sandbox allocation"),
		)?;
		drop(sandbox);
		let runner = self
			.server
			.runner
			.state()
			.id()
			.ok_or_else(|| tg::error!("failed to find the runner id"))?;
		let sandbox = self
			.server
			.runner
			.state()
			.sandboxes()
			.get_by_id(parent_sandbox)
			.ok_or_else(|| tg::error!(%parent_sandbox, "failed to find the parent sandbox"))?;
		let control = sandbox
			.processes
			.get(parent)
			.map(|process| process.control.clone())
			.ok_or_else(|| tg::error!(%parent, "failed to find the parent process"))?;
		drop(sandbox);
		loop {
			let scheduler = self.server.runner.state().wait_for_scheduler().await;
			let allocation = allocation.clone().lock_owned().await;
			let Some((capacity, mut reservation)) = self
				.server
				.runner
				.state()
				.reservations()
				.reserve(allocation, parent_sandbox.clone(), requested)
			else {
				return Ok(());
			};
			let notification = tg::process::control::ClientNotification::BorrowableCapacity(
				tg::process::control::BorrowableCapacityClientNotification {
					capacity,
					parent: parent_sandbox.clone(),
					runner: runner.clone(),
					scheduler: scheduler.clone(),
				},
			);
			control
				.send(tg::process::control::ClientMessage::Notification(
					notification,
				))
				.await
				.map_err(
					|error| tg::error!(!error, %parent, "failed to send the borrowable capacity notification"),
				)?;
			let wait_future = reservation.wait();
			let wait_future = pin!(wait_future);
			let scheduler_change_future = self
				.server
				.runner
				.state()
				.wait_for_scheduler_change(&scheduler);
			let scheduler_change_future = pin!(scheduler_change_future);
			future::select(wait_future, scheduler_change_future).await;
		}
	}

	pub(super) async fn spawn_process_get_command(
		&self,
		arg: &tg::process::spawn::Arg,
		command: &tg::Referent<tg::command::Id>,
	) -> tg::Result<(
		String,
		tg::Referent<tg::Either<Box<tg::process::data::Command>, tg::command::Id>>,
	)> {
		let (host, node) = match &arg.command.node {
			tg::Either::Left(command_arg) => {
				let host = command_arg
					.host
					.clone()
					.ok_or_else(|| tg::error!("expected a resolved host"))?;
				let command = tg::process::data::Command::new(command_arg.clone(), host.clone());
				(host, tg::Either::Left(Box::new(command)))
			},
			tg::Either::Right(id) => {
				let command = tg::Command::with_referent(command.clone());
				self.spawn_process_load_command(&command).await?;
				let data = command.data_with_instance(self).await?;
				(data.host, tg::Either::Right(id.clone()))
			},
		};
		let command = tg::Referent::new(node, arg.command.options.clone());
		let output = (host, command);

		Ok(output)
	}

	async fn spawn_process_load_command(&self, command: &tg::Command) -> tg::Result<()> {
		let output = command.try_load_with_instance(self).await;
		if output.as_ref().is_ok_and(Option::is_some) {
			return Ok(());
		}

		// Spawning requires the command node while its remaining graph can still be transferring.
		let permissions = tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::NODE,
		);
		let storage = tg::storage::Set::Object(tg::object::storage::Set::NODE);
		if self
			.verify(
				command.to_referent().map(tg::object::Id::from),
				permissions,
				storage,
			)
			.await?
			.check_exhaustion()?
			.outcome == crate::authorization::Outcome::Satisfied
		{
			command.load_with_instance(self).await?;
			return Ok(());
		}
		output?.ok_or_else(|| tg::error!("failed to load the command"))?;
		Ok(())
	}

	pub(super) fn spawn_process_is_cacheable(arg: &tg::process::spawn::Arg) -> bool {
		let cacheable = if let Some(tg::Either::Left(sandbox)) = &arg.sandbox {
			sandbox.mounts.is_empty() && sandbox.network.is_none()
		} else {
			false
		};
		let cacheable = cacheable || arg.checksum.is_some();
		cacheable
			&& arg.stdin.is_null()
			&& arg.stdout.is_log()
			&& arg.stderr.is_log()
			&& arg.tty.is_none()
	}

	pub(super) async fn spawn_process_authorize_sandbox_owner(
		&self,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<()> {
		if let Some(tg::Either::Left(sandbox)) = &arg.sandbox {
			self.authorize_owner(sandbox.owner.as_ref()).await?;
		}
		Ok(())
	}

	pub(super) async fn spawn_process_get_or_create_local_process(
		&self,
		arg: &tg::process::spawn::Arg,
		command: &tg::Referent<tg::command::Id>,
		parent_sandbox: Option<&tg::sandbox::Id>,
		cacheable: bool,
		write_command_permissions: bool,
	) -> tg::Result<Option<Output>> {
		if !matches!(arg.cached, Some(true)) {
			let (host, command) = self.spawn_process_get_command(arg, command).await?;
			return self
				.spawn_process_create_local_process(
					arg,
					&command,
					parent_sandbox,
					cacheable,
					write_command_permissions,
					&host,
				)
				.boxed()
				.await
				.map(Some);
		}
		self.try_get_cached_process_local(arg, command)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a cached process"))
	}

	async fn spawn_process_create_local_process(
		&self,
		arg: &tg::process::spawn::Arg,
		command: &tg::Referent<tg::Either<Box<tg::process::data::Command>, tg::command::Id>>,
		parent_sandbox: Option<&tg::sandbox::Id>,
		cacheable: bool,
		write_command_permissions: bool,
		host: &str,
	) -> tg::Result<Output> {
		let requested_owner = match &arg.sandbox {
			Some(tg::Either::Left(sandbox)) => sandbox.owner.clone(),
			Some(tg::Either::Right(sandbox)) => {
				// The verified request origin permits spawning in the same sandbox.
				let origin_owner = self
					.server
					.try_get_request_origin_sandbox(self.context.origin)?
					.filter(|origin| origin.id == sandbox.node)
					.map(|origin| origin.data.arg.owner.clone());
				if let Some(owner) = origin_owner {
					owner
				} else {
					let permission = tg::authorization::Permission::Sandbox(
						tg::authorization::permission::sandbox::Permission::Parent,
					);
					self.authorize(sandbox.clone(), permission)
						.await?
						.into_result()?;
					let sandbox = if let Some(sandbox) =
						self.server.runner.state().try_get_sandbox(&sandbox.node)
					{
						sandbox
					} else {
						self.try_get_sandbox_from_index(&sandbox.node)
							.await?
							.and_then(|sandbox| sandbox.data)
							.ok_or_else(|| tg::error!("failed to find the sandbox"))?
					};
					sandbox.data.owner
				}
			},
			None => return Err(tg::error!("expected the sandbox to be set")),
		};
		let owner = if let Some(parent_sandbox) = parent_sandbox {
			let parent = self.server.runner.state().try_get_sandbox(parent_sandbox);
			let parent = match parent {
				Some(parent) => parent,
				None => self
					.try_get_sandbox_from_index(parent_sandbox)
					.await?
					.and_then(|sandbox| sandbox.data)
					.ok_or_else(
						|| tg::error!(%parent_sandbox, "failed to find the parent sandbox"),
					)?,
			};
			let parent_owner = parent.data.owner;
			let owners_match = requested_owner
				.as_ref()
				.filter(|owner| !matches!(owner, tg::Principal::Root))
				== parent_owner
					.as_ref()
					.filter(|owner| !matches!(owner, tg::Principal::Root));
			if requested_owner.is_some() && !owners_match {
				return Err(tg::error!(
					%parent_sandbox,
					"the child sandbox owner must match the parent sandbox owner"
				));
			}
			parent_owner
		} else if requested_owner.is_some() {
			requested_owner
		} else if matches!(
			self.context.principal,
			tg::Principal::Anonymous | tg::Principal::Root
		) {
			None
		} else {
			Some(self.context.principal.clone())
		};
		if matches!(arg.sandbox, Some(tg::Either::Left(_))) {
			self.verify_billing(owner.as_ref()).await?;
		}

		let id = tg::process::Id::new();
		let now = self.server.clock.unix_timestamp()?;
		let token = self.create_process_wait_token(&id, now)?;
		let process_token = self
			.server
			.create_process_authentication_token(id.clone())?;
		let (sandbox, sandbox_arg, sandbox_token) = match &arg.sandbox {
			Some(tg::Either::Left(sandbox_arg)) => {
				let mut sandbox_arg = Self::normalize_sandbox_create_arg(sandbox_arg.clone())?;
				sandbox_arg.host = Some(host.to_owned());
				sandbox_arg.location.clone_from(&arg.location);
				sandbox_arg.owner.clone_from(&owner);
				let isolation = self.server.resolve_sandbox_isolation()?;
				crate::Server::validate_sandbox_resources(
					&isolation,
					sandbox_arg.cpu,
					sandbox_arg.memory,
					sandbox_arg.hostname.as_deref(),
				)?;
				let sandbox = tg::sandbox::Id::new();
				let token = self
					.server
					.create_sandbox_authentication_token(sandbox.clone())?;
				(sandbox, Some(sandbox_arg), Some(token))
			},
			Some(tg::Either::Right(sandbox)) => (sandbox.node.clone(), None, None),
			None => return Err(tg::error!("expected the sandbox to be set")),
		};
		let tty = arg
			.tty
			.as_ref()
			.map(|tty| {
				tty.as_ref()
					.right()
					.copied()
					.ok_or_else(|| tg::error!("invalid tty"))
			})
			.transpose()?;
		let data = tg::process::Data {
			actual_checksum: None,
			cacheable,
			children: None,
			command: command.clone(),
			created_at: now,
			debug: arg.debug.clone(),
			error: None,
			exit: None,
			expected_checksum: arg.checksum.clone(),
			finished_at: None,
			host: host.to_owned(),
			log: None,
			output: None,
			retry: arg.retry,
			sandbox: Some(sandbox.clone()),
			started_at: Some(now),
			status: tg::process::Status::Started,
			stderr: arg.stderr.clone(),
			stdin: arg.stdin.clone(),
			stdout: arg.stdout.clone(),
			tty,
		};
		let output = Output {
			allocation: None,
			cached: false,
			data,
			id: id.clone(),
			lease: None,
			parent: arg.parent.clone(),
			parent_sandbox: parent_sandbox.cloned(),
			process_token: Some(process_token),
			sandbox_arg,
			sandbox_token,
			scheduler: arg.scheduler.clone(),
			tokens: token.into_iter().collect(),
		};
		// Write the creator permissions independently of the sandbox and prepare command access.
		let mut items = Vec::new();
		if let Some(permission) = self.spawn_process_create_creator_permission_arg(&id, now)? {
			items.push(tangram_index::batch::Item::PutPermission(permission));
		}
		if write_command_permissions {
			let destination = id.clone().into();
			let roots = command
				.objects()
				.into_iter()
				.map(|root| root.map(Into::into));
			items.extend(self.create_capture_permissions_batch_items(
				destination,
				None,
				roots,
				self.context.principal.clone(),
				now,
			)?);
		}
		if !items.is_empty() {
			let arg = tangram_index::batch::Arg { items };
			self.server.index_batch(arg).await.map_err(
				|error| tg::error!(!error, %id, "failed to write the process permissions"),
			)?;
		}

		Ok(output)
	}

	pub(super) fn spawn_process_add_tokens(
		&self,
		output: &mut tg::process::spawn::Output,
	) -> tg::Result<()> {
		if matches!(
			&output.location,
			Some(tg::Location::Local(tg::location::Local { region }))
				if region.as_deref().is_none_or(|region| Some(region) == self.server.config.region.as_deref())
		) && let Some(outcome) = &mut output.outcome
		{
			if let Some(output) = &mut outcome.output {
				self.add_tokens_to_value_data(output)?;
			}
			if let Some(tg::Either::Right(error)) = &mut outcome.error {
				self.add_token_to_object_referent(error)?;
			}
		}
		Ok(())
	}

	pub(in crate::process) async fn spawn_process_add_child(
		&self,
		arg: &tg::process::spawn::Arg,
		output: &tg::process::spawn::Output,
	) -> tg::Result<()> {
		let Some(parent) = &arg.parent else {
			return Ok(());
		};
		let command = output
			.command
			.as_ref()
			.ok_or_else(|| tg::error!("expected the resolved spawn command"))?;
		let child = output.process.as_ref().unwrap_right();
		crate::checkpoint!(
			self.server,
			"process.spawn.child.add",
			cached = output.cached,
			child = %child,
			command = %command,
			parent = %parent,
		)
		.await;
		self.add_process_child(AddProcessChildArg {
			cached: output.cached,
			child,
			command,
			lease: output.lease.as_deref(),
			location: output.location.as_ref(),
			options: &arg.command.options,
			outcome: output.outcome.as_ref(),
			parent,
			tokens: &output.tokens,
		})
		.await
		.map_err(
			|error| tg::error!(!error, %parent, child = %output.process, "failed to add the process as a child"),
		)?;
		Ok(())
	}
}
