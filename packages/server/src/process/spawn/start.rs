use {
	super::local::Output,
	crate::Session,
	futures::{FutureExt as _, TryStreamExt as _, future},
	tangram_client::prelude::*,
	tangram_messenger::Messenger as _,
};

impl Session {
	pub(super) async fn spawn_process_start_local(
		&self,
		arg: &tg::process::spawn::Arg,
		output: Option<&mut Output>,
	) -> tg::Result<()> {
		let Some(output) = output else {
			return Ok(());
		};
		if output.cached || output.data.status.is_finished() {
			return Ok(());
		}
		let mut connected = self
			.server
			.messenger
			.subscribe::<crate::process::control::Connected>(
				crate::process::control::connected_subject(&output.id),
			)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					process = %output.id,
					"failed to subscribe to the process control connection"
				)
			})?;
		let process = output.id.clone();
		let start_future = async {
			match &arg.sandbox {
				Some(tg::Either::Left(_)) => self.spawn_process_in_new_sandbox(output).await?,
				Some(tg::Either::Right(_)) => {
					self.spawn_process_in_existing_sandbox(output).await?;
				},
				None => return Err(tg::error!("expected the sandbox to be set")),
			}
			Ok::<_, tg::Error>(())
		};
		let connect_future = async {
			let connected = connected
				.try_next()
				.await
				.map_err(|error| {
					tg::error!(!error, process = %process, "failed to receive the process control connection")
				})?
				.ok_or_else(|| tg::error!("the process control connection stream ended"))?;
			Ok::<_, tg::Error>(connected.payload.lease)
		};
		let ((), lease) = future::try_join(start_future, connect_future)
			.boxed()
			.await?;
		output.lease = Some(lease);
		Ok(())
	}

	fn spawn_process_runner_arg(output: &Output) -> tg::Result<tg::runner::control::Process> {
		let token = output
			.process_token
			.clone()
			.ok_or_else(|| tg::error!("missing the process token"))?;
		let output = tg::runner::control::Process {
			data: output.data.clone(),
			id: output.id.clone(),
			parent: output.parent.clone(),
			token,
		};
		Ok(output)
	}

	async fn spawn_process_in_existing_sandbox(&self, output: &Output) -> tg::Result<()> {
		let process = Self::spawn_process_runner_arg(output)?;
		let request = tg::sandbox::control::ServerRequestArg::SpawnProcess(
			tg::sandbox::control::SpawnProcessServerRequestArg { process },
		);
		let options = crate::control::Options {
			retry: tangram_futures::retry::Options::default(),
			timeout: std::time::Duration::from_secs(10),
		};
		let response = self
			.send_sandbox_control_request(&output.data.sandbox, request, options)
			.await
			.map_err(|error| {
				tg::error!(!error, sandbox = %output.data.sandbox, process = %output.id, "failed to send the spawn process request")
			})?
			.map_err(|error| {
				tg::error!(!error, sandbox = %output.data.sandbox, process = %output.id, "the spawn process request failed")
			})?;
		response
			.try_unwrap_spawn_process()
			.map_err(|_| tg::error!("expected a spawn process response"))?;
		Ok(())
	}

	async fn spawn_process_in_new_sandbox(&self, output: &mut Output) -> tg::Result<()> {
		let arg = output
			.sandbox_arg
			.clone()
			.ok_or_else(|| tg::error!("missing the sandbox arg"))?;
		let process = Self::spawn_process_runner_arg(output)?;
		if let Some(allocation) = output.allocation.take() {
			let location = self.server.location(arg.location.as_ref())?;
			let data = tg::sandbox::get::Output {
				cpu: arg.cpu,
				creator: Some(self.context.principal.clone()),
				hostname: arg.hostname,
				id: output.data.sandbox.clone(),
				isolation: arg.isolation,
				location: Some(location.clone()),
				memory: arg.memory,
				mounts: arg.mounts,
				network: arg.network,
				owner: arg.owner,
				status: tg::sandbox::Status::Started,
				ttl: arg.ttl,
			};
			self.server
				.spawn_sandbox_task(crate::runner::SpawnSandboxTaskArg {
					allocation,
					data,
					id: output.data.sandbox.clone(),
					location,
					process: Some(process),
					token: output.sandbox_token.clone(),
				});
			return Ok(());
		}
		let request = crate::scheduler::CreateSandboxRequestArg {
			arg,
			creator: Some(self.context.principal.clone()),
			parent: output.parent_sandbox.clone(),
			process: Some(process),
			sandbox: output.data.sandbox.clone(),
			token: output.sandbox_token.clone(),
		};
		self.enqueue_sandbox(request)
			.await
			.map_err(|error| {
				tg::error!(!error, sandbox = %output.data.sandbox, process = %output.id, "failed to enqueue the sandbox")
			})?;
		Ok(())
	}
}
