use {
	super::ProcessControlSender,
	crate::session::Session,
	futures::{StreamExt as _, TryStreamExt as _, future, stream},
	std::{pin::pin, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tokio_util::io::ReaderStream,
};

pub(super) struct RunProcessControlStdinTaskArg {
	pub(super) receiver:
		tokio::sync::mpsc::Receiver<(String, tg::process::control::WriteServerRequestArg)>,
	pub(super) sandbox: tangram_sandbox::Sandbox,
	pub(super) sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub(super) sender: ProcessControlSender,
	pub(super) stdin: tg::process::Stdio,
	pub(super) stdin_blob: Option<tg::Blob>,
}

impl Session {
	pub(super) fn spawn_process_control_stdin_task(
		&self,
		arg: RunProcessControlStdinTaskArg,
	) -> Task<tg::Result<()>> {
		let session = self.clone();
		Task::spawn(move |_| async move { session.run_process_control_stdin_task(arg).await })
	}

	async fn run_process_control_stdin_task(
		&self,
		arg: RunProcessControlStdinTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlStdinTaskArg {
			mut receiver,
			sandbox,
			mut sandbox_process,
			sender,
			stdin,
			stdin_blob,
		} = arg;

		let sandbox_process = sandbox_process
			.wait_for(Option::is_some)
			.await
			.ok()
			.and_then(|sandbox_process| sandbox_process.as_ref().cloned());

		if let Some(blob) = stdin_blob
			&& let Some(sandbox_process) = &sandbox_process
		{
			let reader = blob
				.read_with_handle(self, tg::read::Options::default())
				.await
				.map_err(|error| tg::error!(!error, "failed to read process stdin blob"))?;
			let stream = ReaderStream::new(reader)
				.map_ok(|bytes| {
					tg::process::stdio::read::Event::Chunk(tg::process::stdio::Chunk {
						bytes,
						position: None,
						stream: tg::process::stdio::Stream::Stdin,
					})
				})
				.map_err(|error| tg::error!(!error, "failed to read from the blob"))
				.boxed();
			let stream = sandbox
				.write_stdio(
					sandbox_process,
					vec![tg::process::stdio::Stream::Stdin],
					stream,
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to write stdin"))?;
			let mut stream = pin!(stream);
			while let Some(event) = stream
				.try_next()
				.await
				.map_err(|source| tg::error!(!source, "failed to write stdin"))?
			{
				if matches!(event, tg::process::stdio::write::Event::End) {
					break;
				}
			}
		}
		if !matches!(stdin, tg::process::Stdio::Pipe | tg::process::Stdio::Tty) {
			return Ok(());
		}

		while let Some((id, request)) = receiver.recv().await {
			let response = if let Some(sandbox_process) = &sandbox_process {
				if request.bytes.is_empty() {
					Self::handle_process_control_stdin_close_request(&sandbox, sandbox_process)
						.await
						.map(|()| tg::process::control::WriteClientResponseOutput { length: 0 })
				} else {
					Self::handle_process_control_write_request(&sandbox, sandbox_process, request)
						.await
				}
			} else {
				Ok(tg::process::control::WriteClientResponseOutput { length: 0 })
			};
			let eof = response.as_ref().is_ok_and(|response| response.length == 0);
			let response = response.map(tg::process::control::ClientResponseOutput::Write);
			let response = Self::process_control_response(id.clone(), response);
			sender.send(response).await.ok();
			if eof {
				break;
			}
		}

		Ok(())
	}

	async fn handle_process_control_write_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		write: tg::process::control::WriteServerRequestArg,
	) -> tg::Result<tg::process::control::WriteClientResponseOutput> {
		let chunk = tg::process::stdio::Chunk {
			bytes: write.bytes,
			position: None,
			stream: write.stream,
		};
		let input = stream::once(future::ok(tg::process::stdio::read::Event::Chunk(chunk)));
		let output = sandbox
			.write_stdio(sandbox_process, vec![write.stream], input)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the process stdio"))?;

		// Accumulate the lengths of the writes. A total length of zero indicates that the stream has reached EOF.
		let mut len = 0;
		let mut output = pin!(output);
		while let Some(event) = output.try_next().await? {
			match event {
				tg::process::stdio::write::Event::Write(n) => {
					len += n;
				},
				tg::process::stdio::write::Event::End => {
					break;
				},
				tg::process::stdio::write::Event::Stop => (),
			}
		}
		Ok(tg::process::control::WriteClientResponseOutput { length: len })
	}

	async fn handle_process_control_stdin_close_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
	) -> tg::Result<()> {
		let input = stream::once(future::ok(tg::process::stdio::read::Event::End));
		let output = sandbox
			.write_stdio(
				sandbox_process,
				vec![tg::process::stdio::Stream::Stdin],
				input,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to close the process stdin"))?;
		let mut output = pin!(output);
		while let Some(event) = output.try_next().await? {
			if matches!(event, tg::process::stdio::write::Event::End) {
				break;
			}
		}
		Ok(())
	}
}
