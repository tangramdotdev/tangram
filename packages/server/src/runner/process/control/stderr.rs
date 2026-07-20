use {
	super::{ProcessControlSender, Reader},
	crate::session::Session,
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

pub(super) struct RunProcessControlStderrTaskArg {
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
		Task::spawn(move |_| async move { session.run_process_control_stderr_task(arg).await })
	}

	async fn run_process_control_stderr_task(
		&self,
		arg: RunProcessControlStderrTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlStderrTaskArg {
			mut receiver,
			sandbox,
			mut sandbox_process,
			sender,
			stderr,
			stderr_progress,
		} = arg;

		if !matches!(stderr, tg::process::Stdio::Pipe | tg::process::Stdio::Tty) {
			return Ok(());
		}

		let sandbox_process = sandbox_process
			.wait_for(Option::is_some)
			.await
			.ok()
			.and_then(|sandbox_process| sandbox_process.as_ref().cloned());

		let mut writes = stderr_progress.map(|progress| {
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

		let mut reader = None;
		while let Some((id, request)) = receiver.recv().await {
			let response = if let Some(sandbox_process) = &sandbox_process {
				Self::handle_process_control_stderr_read_request(
					&sandbox,
					sandbox_process,
					request,
					&mut reader,
					&mut writes,
				)
				.await
			} else {
				Ok(tg::process::control::ReadClientResponseOutput {
					stream: request.stream,
					bytes: Bytes::new(),
				})
			};
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

	async fn handle_process_control_stderr_read_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		request: tg::process::control::ReadServerRequestArg,
		reader: &mut Option<Reader>,
		writes: &mut Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>,
	) -> tg::Result<tg::process::control::ReadClientResponseOutput> {
		Self::handle_process_control_read_request(sandbox, sandbox_process, request, reader, writes)
			.await
	}
}
