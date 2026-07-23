use {
	super::child::AddProcessChildArg, crate::Session, futures::FutureExt as _,
	tangram_client::prelude::*,
};

#[derive(derive_more::Debug)]
pub(super) struct Output {
	#[debug(ignore)]
	pub allocation: Option<crate::runner::Allocation>,
	pub cached: bool,
	pub data: tg::process::Data,
	pub id: tg::process::Id,
	pub lease: Option<String>,
	pub parent: Option<tg::process::Id>,
	pub parent_sandbox: Option<tg::sandbox::Id>,
	pub process_token: Option<String>,
	pub sandbox_arg: Option<tg::sandbox::create::Arg>,
	pub sandbox_token: Option<String>,
	pub token: Option<tg::grant::Token>,
}

impl Output {
	pub fn wait(&self) -> tg::Result<Option<tg::process::wait::Output>> {
		if !self.data.status.is_finished() {
			return Ok(None);
		}
		let error = self.data.error.clone().map(|error| match error {
			tg::Either::Left(data) => tg::Either::Left(data),
			tg::Either::Right(id) => {
				let id = id.item;
				tg::Either::Right(id)
			},
		});
		let exit = self
			.data
			.exit
			.ok_or_else(|| tg::error!(process = %self.id, "expected the exit to be set"))?;
		Ok(Some(tg::process::wait::Output {
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
	) -> Option<crate::runner::Allocation> {
		if let Some(parent) = parent
			&& let Some(sandbox) = self.server.runner.state.sandboxes.get(parent)
			&& let Some(allocation) = sandbox.allocation.clone()
			&& let Ok(parent) = allocation.try_lock_owned()
			&& let Some(allocation) = crate::runner::Allocation::try_borrow(parent, requested)
		{
			return Some(allocation);
		}
		self.server.runner.state.capacity.try_acquire(requested)
	}

	pub(crate) fn try_acquire_scheduled_sandbox_capacity(
		&self,
		borrowed: bool,
		parent: Option<&tg::sandbox::Id>,
		requested: tg::runner::Capacity,
	) -> Option<crate::runner::Allocation> {
		if borrowed {
			let parent = parent?;
			return self
				.server
				.runner
				.state
				.reservations
				.try_acquire(parent, requested)
				.or_else(|| self.try_acquire_parent_sandbox_capacity(parent, requested));
		}

		self.try_acquire_sandbox_capacity(parent, requested)
	}

	fn try_acquire_parent_sandbox_capacity(
		&self,
		parent: &tg::sandbox::Id,
		requested: tg::runner::Capacity,
	) -> Option<crate::runner::Allocation> {
		let sandbox = self.server.runner.state.sandboxes.get(parent)?;
		let allocation = sandbox.allocation.clone()?;
		let parent = allocation.try_lock_owned().ok()?;

		crate::runner::Allocation::try_borrow(parent, requested)
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
			.state
			.sandboxes
			.get(parent_sandbox)
			.ok_or_else(|| tg::error!(%parent_sandbox, "failed to find the parent sandbox"))?;
		let allocation = sandbox.allocation.clone().ok_or_else(
			|| tg::error!(%parent_sandbox, "failed to find the parent sandbox allocation"),
		)?;
		drop(sandbox);
		let runner = self
			.server
			.runner
			.state
			.id()
			.ok_or_else(|| tg::error!("failed to find the runner id"))?;
		let sandbox = self
			.server
			.runner
			.state
			.sandboxes
			.get(parent_sandbox)
			.ok_or_else(|| tg::error!(%parent_sandbox, "failed to find the parent sandbox"))?;
		let control = sandbox
			.processes
			.get(parent)
			.map(|process| process.control.clone())
			.ok_or_else(|| tg::error!(%parent, "failed to find the parent process"))?;
		drop(sandbox);
		loop {
			let allocation = allocation.clone().lock_owned().await;
			let Some((capacity, mut reservation)) = self.server.runner.state.reservations.reserve(
				allocation,
				parent_sandbox.clone(),
				requested,
			) else {
				return Ok(());
			};
			let notification = tg::process::control::ClientNotification::BorrowableCapacity(
				tg::process::control::BorrowableCapacityClientNotification {
					capacity,
					parent: parent_sandbox.clone(),
					runner: runner.clone(),
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
			reservation.wait().await;
		}
	}

	pub(super) async fn spawn_process_authorize_command(
		&self,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<()> {
		let permission =
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree);
		let command = arg.command.clone().map(tg::object::Id::from);
		if !self
			.authorize(command, permission)
			.await?
			.is_some_and(|permissions| permissions.contains(permission))
		{
			return Err(tg::error!("unauthorized"));
		}
		Ok(())
	}

	pub(super) async fn spawn_process_get_command_host(
		&self,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<String> {
		let command = tg::Command::with_referent(arg.command.clone());
		let host = command
			.host_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the command host"))?
			.to_string();
		Ok(host)
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
		parent_sandbox: Option<&tg::sandbox::Id>,
		cacheable: bool,
	) -> tg::Result<Option<Output>> {
		if !matches!(arg.cached, Some(true)) {
			let host = self.spawn_process_get_command_host(arg).await?;
			return self
				.spawn_process_create_local_process(arg, parent_sandbox, cacheable, &host)
				.boxed()
				.await
				.map(Some);
		}
		self.try_get_cached_process_local(arg)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a cached process"))
	}

	async fn spawn_process_create_local_process(
		&self,
		arg: &tg::process::spawn::Arg,
		parent_sandbox: Option<&tg::sandbox::Id>,
		cacheable: bool,
		host: &str,
	) -> tg::Result<Output> {
		let owner = match &arg.sandbox {
			Some(tg::Either::Left(sandbox)) => sandbox.owner.clone(),
			Some(tg::Either::Right(sandbox)) => {
				self.try_get_sandbox(
					sandbox,
					tg::sandbox::get::Arg {
						location: arg.location.clone(),
					},
				)
				.boxed()
				.await?
				.ok_or_else(|| tg::error!("failed to find the sandbox"))?
				.owner
			},
			None => return Err(tg::error!("expected the sandbox to be set")),
		};
		let owner = if owner.is_some() {
			owner
		} else if let Some(parent_sandbox) = parent_sandbox {
			self.server
				.runner
				.state
				.try_get_sandbox(parent_sandbox)
				.and_then(|sandbox| sandbox.owner)
		} else if matches!(
			self.context.principal,
			tg::Principal::Anonymous | tg::Principal::Root
		) {
			None
		} else {
			Some(self.context.principal.clone())
		};

		let id = tg::process::Id::new();
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
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
			Some(tg::Either::Right(sandbox)) => (sandbox.clone(), None, None),
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
			command: arg.command.item.clone(),
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
			sandbox: sandbox.clone(),
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
			id,
			lease: None,
			parent: arg.parent.clone(),
			parent_sandbox: parent_sandbox.cloned(),
			process_token: Some(process_token),
			sandbox_arg,
			sandbox_token,
			token,
		};
		Ok(output)
	}

	pub(super) fn spawn_process_add_tokens(
		&self,
		output: &mut tg::process::spawn::Output,
	) -> tg::Result<()> {
		if matches!(
			output.location,
			Some(tg::Location::Local(tg::location::Local { region: None }))
		) && let Some(wait) = &mut output.wait
			&& let Some(output) = &mut wait.output
		{
			self.add_tokens_to_value_data(output)?;
		}
		Ok(())
	}

	pub(super) async fn spawn_process_add_child(
		&self,
		arg: &tg::process::spawn::Arg,
		output: &tg::process::spawn::Output,
	) -> tg::Result<()> {
		let Some(parent) = &arg.parent else {
			return Ok(());
		};
		if !self.server.runner.state.processes.contains_key(parent) {
			return Ok(());
		}
		let child = output.process.as_ref().unwrap_right();
		let sandbox = self.server.runner.state.try_get_process_sandbox(child);
		self.add_process_child(AddProcessChildArg {
			cached: output.cached,
			child,
			command: &arg.command.item,
			lease: output.lease.as_deref(),
			location: output.location.as_ref(),
			options: &arg.command.options,
			parent,
			sandbox: sandbox.as_ref(),
			token: output.token.as_ref(),
		})
		.await
		.map_err(
			|error| tg::error!(!error, %parent, child = %output.process, "failed to add the process as a child"),
		)?;
		Ok(())
	}
}
