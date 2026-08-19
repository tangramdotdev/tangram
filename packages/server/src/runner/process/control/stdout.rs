use {
	super::ProcessControlSender,
	crate::session::Session,
	bytes::Bytes,
	futures::{StreamExt as _, TryFutureExt as _, TryStreamExt as _, stream::BoxStream},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

pub(super) struct RunProcessControlStdoutTaskArg {
	pub(super) buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
	pub(super) progress: Option<BoxStream<'static, tg::Result<Bytes>>>,
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
			buffered,
			progress,
			receiver,
			sandbox,
			mut sandbox_process,
			sender,
			stdout,
		} = arg;

		if !matches!(stdout, tg::process::Stdio::Pipe | tg::process::Stdio::Tty) {
			buffered.send(Ok(())).ok();

			return Ok(());
		}

		let sandbox_process = sandbox_process
			.wait_for(Option::is_some)
			.await
			.ok()
			.and_then(|sandbox_process| sandbox_process.as_ref().cloned());

		let writes = progress.map(|progress| {
			progress
				.map_ok(|bytes| {
					tg::process::stdio::read::Event::Chunk(tg::process::stdio::Chunk {
						bytes,
						position: None,
						stream: tg::process::stdio::Stream::Stderr,
					})
				})
				.boxed()
		});
		let reader = self
			.create_process_control_reader(
				buffered,
				&sandbox,
				sandbox_process.as_deref(),
				tg::process::stdio::Stream::Stdout,
				writes,
			)
			.await?;
		Self::run_process_control_reader_task(reader, receiver, sender).await
	}
}
