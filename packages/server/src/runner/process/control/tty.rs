use {
	super::ProcessControlSender, crate::session::Session, std::sync::Arc,
	tangram_client::prelude::*, tangram_futures::task::Task,
};

pub(super) struct RunProcessControlTtyTaskArg {
	pub(super) receiver:
		tokio::sync::mpsc::Receiver<(String, tg::process::control::TtyServerRequestArg)>,
	pub(super) sandbox: tangram_sandbox::Sandbox,
	pub(super) sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub(super) sender: ProcessControlSender,
}

impl Session {
	pub(super) fn spawn_process_control_tty_task(
		&self,
		arg: RunProcessControlTtyTaskArg,
	) -> Task<tg::Result<()>> {
		let session = self.clone();
		Task::spawn(move |_| async move { session.run_process_control_tty_task(arg).await })
	}

	async fn run_process_control_tty_task(
		&self,
		arg: RunProcessControlTtyTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlTtyTaskArg {
			mut receiver,
			sandbox,
			mut sandbox_process,
			sender,
		} = arg;

		let sandbox_process = sandbox_process
			.wait_for(Option::is_some)
			.await
			.ok()
			.and_then(|sandbox_process| sandbox_process.as_ref().cloned());

		while let Some((id, request)) = receiver.recv().await {
			let result = if let Some(sandbox_process) = &sandbox_process {
				Self::handle_process_control_tty_request(&sandbox, sandbox_process, request).await
			} else {
				Err(tg::error!("the process was not spawned"))
			};
			let response = result.map(|()| {
				tg::process::control::ClientResponseOutput::Tty(
					tg::process::control::TtyClientResponseOutput {},
				)
			});
			let response = Self::process_control_response(id.clone(), response);
			sender.send(response).await.ok();
		}

		Ok(())
	}

	async fn handle_process_control_tty_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		tty: tg::process::control::TtyServerRequestArg,
	) -> tg::Result<()> {
		sandbox
			.set_tty_size(sandbox_process, tty.size)
			.await
			.map_err(|error| tg::error!(!error, "failed to set the tty size"))?;
		Ok(())
	}
}
