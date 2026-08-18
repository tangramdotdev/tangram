use {
	super::ProcessControlSender, crate::session::Session, futures::TryFutureExt as _,
	std::sync::Arc, tangram_client::prelude::*, tangram_futures::task::Task,
};

pub(super) struct RunProcessControlSignalTaskArg {
	pub(super) receiver:
		tokio::sync::mpsc::Receiver<(String, tg::process::control::SignalServerRequestArg)>,
	pub(super) sandbox: tangram_sandbox::Sandbox,
	pub(super) sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub(super) sender: ProcessControlSender,
}

impl Session {
	pub(super) fn spawn_process_control_signal_task(
		&self,
		arg: RunProcessControlSignalTaskArg,
	) -> Task<tg::Result<()>> {
		let session = self.clone();
		Task::spawn(move |_| {
			async move { session.run_process_control_signal_task(arg).await }.inspect_err(
				|error| tracing::error!(error = %error.trace(), "the process control signal task failed"),
			)
		})
	}

	async fn run_process_control_signal_task(
		&self,
		arg: RunProcessControlSignalTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlSignalTaskArg {
			mut receiver,
			sandbox,
			mut sandbox_process,
			sender,
		} = arg;

		let sandbox_process = sandbox_process
			.wait_for(Option::is_some)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the sandboxed process"))?
			.as_ref()
			.cloned()
			.ok_or_else(|| tg::error!("failed to get the sandboxed process"))?;

		while let Some((id, request)) = receiver.recv().await {
			let result =
				Self::handle_process_control_signal_request(&sandbox, &sandbox_process, request)
					.await;
			let response = result.map(|()| {
				tg::process::control::ClientResponseOutput::Signal(
					tg::process::control::SignalClientResponseOutput {},
				)
			});
			let response = Self::process_control_response(id.clone(), response);
			sender.send(response).await.ok();
		}

		Ok(())
	}

	async fn handle_process_control_signal_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		signal: tg::process::control::SignalServerRequestArg,
	) -> tg::Result<()> {
		sandbox
			.kill(sandbox_process, signal.signal)
			.await
			.map_err(|error| tg::error!(!error, "failed to signal the process"))?;
		Ok(())
	}
}
