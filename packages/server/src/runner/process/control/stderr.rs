use {
	super::ProcessControlSender,
	crate::session::Session,
	bytes::Bytes,
	futures::{StreamExt as _, TryFutureExt as _, TryStreamExt as _, stream::BoxStream},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

pub(super) struct RunProcessControlStderrTaskArg {
	pub(super) buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
	pub(super) receiver:
		tokio::sync::mpsc::Receiver<(String, tg::process::control::ReadServerRequestArg)>,
	pub(super) sandbox: tangram_sandbox::Sandbox,
	pub(super) sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub(super) sender: ProcessControlSender,
	pub(super) stderr: tg::process::Stdio,
	pub(super) stderr_progress: Option<BoxStream<'static, tg::Result<Bytes>>>,
}

impl Session {
	pub(super) fn spawn_process_control_stderr_task(
		&self,
		arg: RunProcessControlStderrTaskArg,
	) -> Task<tg::Result<()>> {
		let session = self.clone();
		Task::spawn(move |_| {
			async move { session.run_process_control_stderr_task(arg).await }.inspect_err(
				|error| tracing::error!(error = %error.trace(), "the process control stderr task failed"),
			)
		})
	}

	async fn run_process_control_stderr_task(
		&self,
		arg: RunProcessControlStderrTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlStderrTaskArg {
			buffered,
			receiver,
			sandbox,
			mut sandbox_process,
			sender,
			stderr,
			stderr_progress,
		} = arg;

		if !matches!(stderr, tg::process::Stdio::Pipe | tg::process::Stdio::Tty) {
			buffered.send(Ok(())).ok();

			return Ok(());
		}

		let sandbox_process = sandbox_process
			.wait_for(Option::is_some)
			.await
			.ok()
			.and_then(|sandbox_process| sandbox_process.as_ref().cloned());

		let writes = stderr_progress.map(|progress| {
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
				tg::process::stdio::Stream::Stderr,
				writes,
			)
			.await?;
		Self::run_process_control_reader_task(reader, receiver, sender).await
	}
}
