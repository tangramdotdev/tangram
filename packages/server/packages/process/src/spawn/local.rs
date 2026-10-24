use {
	super::child::AddProcessChildArg,
	crate::Component,
	futures::{FutureExt as _, future},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_server_core::Session,
};

#[derive(derive_more::Debug)]
pub(super) struct Output {
	#[debug(ignore)]
	pub allocation: Option<tangram_server_runner_core::capacity::Allocation>,

	#[debug(ignore)]
	pub open: Option<crate::connect::service::Open>,

	pub cached: bool,

	pub data: tg::process::Data,

	pub id: tg::process::Id,

	pub lease: Option<tg::process::Lease>,

	pub parent: Option<tg::process::Id>,

	pub parent_sandbox: Option<tg::sandbox::Id>,

	pub process_token: Option<String>,

	pub sandbox_arg: Option<tg::sandbox::create::Arg>,

	pub sandbox_token: Option<String>,

	pub scheduler: Option<tg::scheduler::Id>,

	pub tokens: tg::authorization::Tokens,
}

struct SpawnProcessCreateLocalProcessArg<'a> {
	cacheable: bool,
	command: &'a tg::Referent<tg::Either<Box<tg::process::data::Command>, tg::command::Id>>,
	host: &'a str,
	parent_sandbox: Option<&'a tg::sandbox::Id>,
	spawn_arg: &'a tg::process::spawn::Arg,
	write_command_permissions: bool,
}

impl Output {
	pub fn try_outcome(&self) -> tg::Result<Option<tg::process::outcome::Data>> {
		if !self.data.status.is_finished() {
			return Ok(None);
		}
		let error = self.data.error_with_location_and_tokens();
		let exit = self
			.data
			.exit
			.ok_or_else(|| tg::error!(process = %self.id, "expected the exit to be set"))?;
		Ok(Some(tg::process::outcome::Data {
			checksum: self.data.checksum.actual.clone(),
			error,
			exit,
			output: self.data.output_with_location_and_tokens(),
		}))
	}
}

impl Component {
	pub(crate) fn try_acquire_sandbox_capacity(
		session: &Session,
		parent: Option<&tg::sandbox::Id>,
		requested: tg::runner::Capacity,
	) -> Option<tangram_server_runner_core::capacity::Allocation> {
		if let Some(parent) = parent
			&& let Some(sandbox) = session
				.server
				.components
				.runner
				.state()
				.sandboxes()
				.get_by_id(parent)
			&& let Some(allocation) = sandbox.allocation.clone()
			&& let Ok(parent) = allocation.try_lock_owned()
			&& let Some(allocation) = tangram_server_runner_core::capacity::Allocation::try_borrow(
				session.server.components.runner.state().capacity(),
				parent,
				requested,
			) {
			return Some(allocation);
		}
		session
			.server
			.components
			.runner
			.state()
			.capacity()
			.try_acquire(requested)
	}

	pub(crate) fn try_acquire_scheduled_sandbox_capacity(
		session: &Session,
		borrowed: bool,
		parent: Option<&tg::sandbox::Id>,
		requested: tg::runner::Capacity,
	) -> Option<tangram_server_runner_core::capacity::Allocation> {
		if borrowed {
			let parent = parent?;
			return session
				.server
				.components
				.runner
				.state()
				.reservations()
				.try_acquire(
					session.server.components.runner.state().capacity(),
					parent,
					requested,
				)
				.or_else(|| Self::try_acquire_parent_sandbox_capacity(session, parent, requested));
		}

		Self::try_acquire_sandbox_capacity(session, parent, requested)
	}

	fn try_acquire_parent_sandbox_capacity(
		session: &Session,
		parent: &tg::sandbox::Id,
		requested: tg::runner::Capacity,
	) -> Option<tangram_server_runner_core::capacity::Allocation> {
		let sandbox = session
			.server
			.components
			.runner
			.state()
			.sandboxes()
			.get_by_id(parent)?;
		let allocation = sandbox.allocation.clone()?;
		let parent = allocation.try_lock_owned().ok()?;

		tangram_server_runner_core::capacity::Allocation::try_borrow(
			session.server.components.runner.state().capacity(),
			parent,
			requested,
		)
	}

	pub(super) async fn spawn_process_notify_borrowable_capacity(
		&self,
		session: &Session,
		parent: &tg::process::Id,
		parent_sandbox: &tg::sandbox::Id,
		requested: tg::runner::Capacity,
	) -> tg::Result<()> {
		let sandbox = session
			.server
			.components
			.runner
			.state()
			.sandboxes()
			.get_by_id(parent_sandbox)
			.ok_or_else(|| tg::error!(%parent_sandbox, "failed to find the parent sandbox"))?;
		let capacity = sandbox.capacity;
		let allocation = sandbox.allocation.clone().ok_or_else(
			|| tg::error!(%parent_sandbox, "failed to find the parent sandbox allocation"),
		)?;
		drop(sandbox);
		let runner = session
			.server
			.components
			.runner
			.state()
			.id()
			.ok_or_else(|| tg::error!("failed to find the runner id"))?;
		let sandbox = session
			.server
			.components
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
			let scheduler = session
				.server
				.components
				.runner
				.state()
				.wait_for_scheduler()
				.await;
			if capacity.try_borrow(requested).is_none() {
				return Ok(());
			}

			// Report potential capacity before waiting for an outstanding borrower to finish.
			let notification = tg::process::control::BorrowableCapacityClientNotification {
				available: false,
				capacity,
				heartbeat_index: session.server.components.runner.state().heartbeat_index(),
				parent: parent_sandbox.clone(),
				runner: runner.clone(),
				scheduler: scheduler.clone(),
			};
			self.spawn_process_send_borrowable_capacity(&control, notification)
				.await?;
			let allocation = allocation.clone().lock_owned().await;
			let Some((capacity, mut reservation)) = session
				.server
				.components
				.runner
				.state()
				.reservations()
				.reserve(allocation, parent_sandbox.clone(), requested)
			else {
				let notification = tg::process::control::BorrowableCapacityClientNotification {
					available: false,
					capacity: tg::runner::Capacity::default(),
					heartbeat_index: session.server.components.runner.state().heartbeat_index(),
					parent: parent_sandbox.clone(),
					runner: runner.clone(),
					scheduler: scheduler.clone(),
				};
				self.spawn_process_send_borrowable_capacity(&control, notification)
					.await?;
				return Ok(());
			};
			let notification = tg::process::control::BorrowableCapacityClientNotification {
				available: true,
				capacity,
				heartbeat_index: session.server.components.runner.state().heartbeat_index(),
				parent: parent_sandbox.clone(),
				runner: runner.clone(),
				scheduler: scheduler.clone(),
			};
			self.spawn_process_send_borrowable_capacity(&control, notification)
				.await?;
			let wait_future = reservation.wait();
			let wait_future = pin!(wait_future);
			let scheduler_change_future = session
				.server
				.components
				.runner
				.state()
				.wait_for_scheduler_change(&scheduler);
			let scheduler_change_future = pin!(scheduler_change_future);
			future::select(wait_future, scheduler_change_future).await;
		}
	}

	async fn spawn_process_send_borrowable_capacity(
		&self,
		control: &tg::process::control::Sender,
		notification: tg::process::control::BorrowableCapacityClientNotification,
	) -> tg::Result<()> {
		let parent = notification.parent.clone();
		let notification =
			tg::process::control::ClientNotification::BorrowableCapacity(notification);
		control
			.send(tg::process::control::ClientMessage::Notification(
				notification,
			))
			.await
			.map_err(
				|error| tg::error!(!error, %parent, "failed to send the borrowable capacity notification"),
			)?;
		Ok(())
	}

	pub(super) async fn spawn_process_get_command(
		&self,
		session: &Session,
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
				self.spawn_process_load_command(session, &command).await?;
				let data = command.data_with_instance(session).await?;
				(data.host, tg::Either::Right(id.clone()))
			},
		};
		let command = tg::Referent::new(node, arg.command.options.clone());
		let output = (host, command);

		Ok(output)
	}

	async fn spawn_process_load_command(
		&self,
		session: &Session,
		command: &tg::Command,
	) -> tg::Result<()> {
		let output = command.try_load_with_instance(session).await;
		if output.as_ref().is_ok_and(Option::is_some) {
			return Ok(());
		}

		// Spawning requires the command node while its remaining graph can still be transferring.
		let permissions = tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::NODE,
		);
		let storage = tg::storage::Set::Object(tg::object::storage::Set::NODE);
		if session
			.verify(
				command.to_referent().map(tg::object::Id::from),
				permissions,
				storage,
				None,
			)
			.await?
			.check_exhaustion()?
			.outcome == tangram_server_core::authorization::Outcome::Satisfied
		{
			command.load_with_instance(session).await?;
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
		session: &Session,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<()> {
		if let Some(tg::Either::Left(sandbox)) = &arg.sandbox {
			session
				.server
				.components
				.authorization
				.authorize_owner(session, sandbox.owner.as_ref())
				.await?;
		}
		Ok(())
	}

	pub(super) async fn spawn_process_get_or_create_local_process(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		command: &tg::Referent<tg::command::Id>,
		parent_sandbox: Option<&tg::sandbox::Id>,
		cacheable: bool,
		write_command_permissions: bool,
	) -> tg::Result<Option<Output>> {
		if !matches!(arg.cached, Some(true)) {
			let (host, command) = self
				.spawn_process_get_command(session, arg, command)
				.await?;
			let arg = SpawnProcessCreateLocalProcessArg {
				cacheable,
				command: &command,
				host: &host,
				parent_sandbox,
				spawn_arg: arg,
				write_command_permissions,
			};
			return self
				.spawn_process_create_local_process(session, arg)
				.boxed()
				.await
				.map(Some);
		}
		self.try_get_cached_process_local(session, arg, command)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a cached process"))
	}

	async fn spawn_process_create_local_process(
		&self,
		session: &Session,
		arg: SpawnProcessCreateLocalProcessArg<'_>,
	) -> tg::Result<Output> {
		let SpawnProcessCreateLocalProcessArg {
			cacheable,
			command,
			host,
			parent_sandbox,
			spawn_arg,
			write_command_permissions,
		} = arg;

		// Resolve the process owner.
		let requested_owner = match &spawn_arg.sandbox {
			Some(tg::Either::Left(sandbox)) => sandbox.owner.clone(),
			Some(tg::Either::Right(sandbox)) => {
				// The verified request origin permits spawning in the same sandbox.
				let origin_owner = session
					.server
					.try_get_request_origin_sandbox(session.context.origin)?
					.filter(|origin| origin.id == sandbox.node)
					.map(|origin| origin.create.owner.clone());
				if let Some(owner) = origin_owner {
					owner
				} else {
					let permission = tg::authorization::Permission::Sandbox(
						tg::authorization::permission::sandbox::Permission::Parent,
					);
					session
						.authorize(sandbox.clone(), permission)
						.await?
						.into_result()?;
					let sandbox = if let Some(sandbox) = session
						.server
						.components
						.runner
						.state()
						.try_get_sandbox(&sandbox.node)
					{
						sandbox
					} else {
						session
							.server
							.components
							.sandbox
							.try_get_sandbox_from_index(session, &sandbox.node)
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
			let parent = session
				.server
				.components
				.runner
				.state()
				.try_get_sandbox(parent_sandbox);
			let parent = match parent {
				Some(parent) => parent,
				None => session
					.server
					.components
					.sandbox
					.try_get_sandbox_from_index(session, parent_sandbox)
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
			session.context.principal,
			tg::Principal::Anonymous | tg::Principal::Root
		) {
			None
		} else {
			Some(session.context.principal.clone())
		};
		if matches!(spawn_arg.sandbox, Some(tg::Either::Left(_))) {
			session
				.server
				.components
				.billing
				.verify_billing(session, owner.as_ref())
				.await?;
		}

		// Create the process identity and tokens.
		let id = tg::process::Id::new();
		let now = session.server.clock.unix_timestamp();
		let token = Self::create_process_wait_token(session, &id, now)?;
		let process_token = session
			.server
			.components
			.authentication
			.create_process_authentication_token(&session.server, id.clone())?;

		// Resolve the sandbox and terminal settings.
		let (sandbox, sandbox_arg, sandbox_token) = match &spawn_arg.sandbox {
			Some(tg::Either::Left(sandbox_arg)) => {
				let mut sandbox_arg =
					tangram_server_sandbox_core::normalize_sandbox_create_arg(sandbox_arg.clone())?;
				sandbox_arg.host = Some(host.to_owned());
				sandbox_arg.location.clone_from(&spawn_arg.location);
				sandbox_arg.owner.clone_from(&owner);
				tangram_server_sandbox_core::clear_darwin_sandbox_resources(&mut sandbox_arg);
				let isolation = session
					.server
					.components
					.runner
					.resolve_sandbox_backend(&session.server)?;
				tangram_server_sandbox_core::validate_sandbox_resources(
					&isolation,
					sandbox_arg.cpu,
					sandbox_arg.memory,
					sandbox_arg.hostname.as_deref(),
					sandbox_arg.storage,
				)?;
				let sandbox = tg::sandbox::Id::new();
				let token = session
					.server
					.components
					.authentication
					.create_sandbox_authentication_token(&session.server, sandbox.clone())?;
				(sandbox, Some(sandbox_arg), Some(token))
			},
			Some(tg::Either::Right(sandbox)) => (sandbox.node.clone(), None, None),
			None => return Err(tg::error!("expected the sandbox to be set")),
		};
		let tty = spawn_arg
			.tty
			.as_ref()
			.map(|tty| {
				tty.as_ref()
					.right()
					.copied()
					.ok_or_else(|| tg::error!("invalid tty"))
			})
			.transpose()?;

		// Create the process data and response.
		let checksum = tg::process::Checksum {
			actual: None,
			expected: spawn_arg.checksum.clone(),
		};
		let data = tg::process::Data {
			cacheable,
			checksum,
			children: None,
			command: command.clone(),
			created_at: now,
			debug: spawn_arg.debug.clone(),
			error: None,
			exit: None,
			finished_at: None,
			host: host.to_owned(),
			log: None,
			output: None,
			retry: spawn_arg.retry,
			sandbox: Some(sandbox.clone()),
			started_at: Some(now),
			status: tg::process::Status::Started,
			stderr: spawn_arg.stderr.clone(),
			stdin: spawn_arg.stdin.clone(),
			stdout: spawn_arg.stdout.clone(),
			tty,
		};
		let output = Output {
			allocation: None,
			open: None,
			cached: false,
			data,
			id: id.clone(),
			lease: None,
			parent: spawn_arg.parent.clone(),
			parent_sandbox: parent_sandbox.cloned(),
			process_token: Some(process_token),
			sandbox_arg,
			sandbox_token,
			scheduler: spawn_arg.scheduler.clone(),
			tokens: tg::authorization::Tokens::with_authorization(token),
		};

		// Write the creator permissions independently of the sandbox and prepare command access.
		let mut items = Vec::new();
		if let Some(permission) =
			Self::spawn_process_create_creator_permission_arg(session, &id, now)?
		{
			items.push(tangram_index::batch::Item::PutPermission(permission));
		}
		if write_command_permissions {
			let destination = id.clone().into();
			let roots = command
				.objects()
				.into_iter()
				.map(|root| root.map(Into::into));
			items.extend(
				session
					.server
					.components
					.authorization
					.create_capture_permissions_batch_items(
						session,
						destination,
						None,
						roots.into_iter().collect(),
						session.context.principal.clone(),
						now,
					)?,
			);
		}
		if !items.is_empty() {
			let arg = tangram_index::batch::Arg { items };
			session
				.server
				.components
				.indexer
				.index_batch(&session.server, arg)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to write the process permissions"),
				)?;
		}

		Ok(output)
	}

	pub(super) fn spawn_process_add_tokens(
		session: &Session,
		output: &mut tg::process::spawn::Output,
	) -> tg::Result<()> {
		if matches!(
			&output.location,
			Some(tg::Location::Local(tg::location::Local { region }))
				if region.as_deref().is_none_or(|region| Some(region) == session.server.config.region.as_deref())
		) && let Some(outcome) = &mut output.outcome
		{
			if let Some(output) = &mut outcome.output {
				session
					.server
					.components
					.authorization
					.add_tokens_to_value_data(session, output)?;
			}
			if let Some(tg::Either::Right(error)) = &mut outcome.error {
				session.add_token_to_object_referent(error)?;
			}
		}
		Ok(())
	}

	pub(crate) async fn spawn_process_add_child(
		&self,
		session: &Session,
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
		tangram_server_core::checkpoint!(
			session.server,
			"process.spawn.child.add",
			cached = output.cached,
			child = %child,
			command = %command,
			parent = %parent,
		)
		.await;
		self.add_process_child(session, AddProcessChildArg {
			cached: output.cached,
			child,
			command,
			lease: output.lease.as_ref(),
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
