use {
	super::{ProcessControlSender, Reader},
	crate::session::Session,
	futures::TryFutureExt as _,
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

pub(super) struct RunProcessControlStdoutTaskArg {
	pub(super) receiver:
		tokio::sync::mpsc::Receiver<(String, tg::process::control::ReadServerRequestArg)>,
	pub(super) sandbox: tangram_sandbox::Sandbox,
	pub(super) sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub(super) sender: ProcessControlSender,
	pub(super) stdout: tg::process::Stdio,
}

impl Session {
	pub(super) fn spawn_process_control_stdout_task(
		&self,
		arg: RunProcessControlStdoutTaskArg,
	) -> Task<tg::Result<()>> {
		let session = self.clone();
		Task::spawn(move |_| {
			async move { session.run_process_control_stdout_task(arg).await }.inspect_err(
				|error| tracing::error!(error = %error.trace(), "the process control stdout task failed"),
			)
		})
	}

	async fn run_process_control_stdout_task(
		&self,
		arg: RunProcessControlStdoutTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlStdoutTaskArg {
			mut receiver,
			sandbox,
			mut sandbox_process,
			sender,
			stdout,
		} = arg;

		if !matches!(stdout, tg::process::Stdio::Pipe | tg::process::Stdio::Tty) {
			return Ok(());
		}

		let sandbox_process = sandbox_process
			.wait_for(Option::is_some)
			.await
			.ok()
			.and_then(|sandbox_process| sandbox_process.as_ref().cloned());

		let mut writes = None;
		let mut reader = Self::create_process_control_reader(
			&sandbox,
			sandbox_process.as_deref(),
			tg::process::stdio::Stream::Stdout,
			&mut writes,
		)
		.await?;
		while let Some((id, request)) = receiver.recv().await {
			let response =
				Self::handle_process_control_stdout_read_request(request, &mut reader).await;
			let eof = response
				.as_ref()
				.is_ok_and(|response| response.bytes.is_empty());
			let response = response.map(tg::process::control::ClientResponseOutput::Read);
			let response = Self::process_control_response(id.clone(), response);
			sender.send(response).await.ok();
			if eof {
				break;
			}
		}

		Ok(())
	}

	async fn handle_process_control_stdout_read_request(
		request: tg::process::control::ReadServerRequestArg,
		reader: &mut Reader,
	) -> tg::Result<tg::process::control::ReadClientResponseOutput> {
		Self::handle_process_control_read_request(request, reader).await
	}
}
