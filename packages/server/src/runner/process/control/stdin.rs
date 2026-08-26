use {
	super::ProcessControlSender,
	crate::session::Session,
	futures::{StreamExt as _, TryFutureExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::{pin::pin, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tokio_util::io::ReaderStream,
};

pub(super) struct RunProcessControlStdinTaskArg {
	pub(super) finished: Stopper,
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
		Task::spawn(move |_| {
			async move { session.run_process_control_stdin_task(arg).await }.inspect_err(
				|error| tracing::error!(error = %error.trace(), "the process control stdin task failed"),
			)
		})
	}

	async fn run_process_control_stdin_task(
		&self,
		arg: RunProcessControlStdinTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlStdinTaskArg {
			finished,
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
					tangram_sandbox::stdio::read::Event::Chunk(tangram_sandbox::stdio::Chunk {
						bytes,
						stream: tg::process::stdio::Stream::Stdin,
					})
				})
				.map_err(|error| tg::error!(!error, "failed to read from the blob"))
				.chain(stream::once(future::ok(
					tangram_sandbox::stdio::read::Event::End,
				)))
				.boxed();
			let output = sandbox
				.write_stdio(
					sandbox_process,
					vec![tg::process::stdio::Stream::Stdin],
					stream,
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to write stdin"))?;
			let mut output = pin!(output);
			while let Some(event) = output.try_next().await? {
				if matches!(event, tangram_sandbox::stdio::write::Event::End) {
					break;
				}
			}
		}
		if !matches!(stdin, tg::process::Stdio::Pipe | tg::process::Stdio::Tty) {
			return Ok(());
		}
		let mut closed = false;
		let mut position = 0_u64;
		while let Some((id, request)) = receiver.recv().await {
			crate::checkpoint!(
				self.server,
				"runner.process.control.stdin.write",
				close = %request.chunk.bytes.is_empty(),
			)
			.await;

			// Once the process has finished, its sandbox is torn down and cannot be written to, so answer every write with the current position.
			let result = if closed || finished.stopped() {
				Ok(position)
			} else if let Some(sandbox_process) = &sandbox_process {
				Self::handle_process_control_stdin_write_request(
					&sandbox,
					sandbox_process,
					request,
					&mut position,
					&mut closed,
				)
				.await
			} else {
				Ok(position)
			};
			let response = result
				.map(|position| tg::process::control::WriteClientResponseOutput { position })
				.map(tg::process::control::ClientResponseOutput::Write);
			let response = Self::process_control_response(id, response);
			sender.send(response).await?;
		}

		Ok(())
	}

	async fn handle_process_control_stdin_write_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		request: tg::process::control::WriteServerRequestArg,
		position: &mut u64,
		closed: &mut bool,
	) -> tg::Result<u64> {
		let mut chunk = request.chunk;
		if chunk.stream != tg::process::stdio::Stream::Stdin {
			return Err(tg::error!("invalid process stdio stream"));
		}
		let start = chunk.stream_position;
		let end = start
			.checked_add(chunk.bytes.len().to_u64().unwrap())
			.ok_or_else(|| tg::error!("the stdin position is too large"))?;
		if start > *position {
			return Err(tg::error!(
				expected = %*position,
				actual = %start,
				"encountered a gap in the stdin stream"
			));
		}
		if chunk.bytes.is_empty() {
			Self::handle_process_control_stdin_close_request(sandbox, sandbox_process).await?;
			*closed = true;

			return Ok(*position);
		}
		if end <= *position {
			return Ok(*position);
		}
		if start < *position {
			let offset = (*position - start).to_usize().unwrap();
			chunk.bytes = chunk.bytes.slice(offset..);
		}
		let input = stream::once(future::ok(tangram_sandbox::stdio::read::Event::Chunk(
			tangram_sandbox::stdio::Chunk {
				bytes: chunk.bytes,
				stream: chunk.stream,
			},
		)));
		let output = sandbox
			.write_stdio(sandbox_process, vec![chunk.stream], input)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the process stdio"))?;
		let mut output = pin!(output);
		while let Some(event) = output.try_next().await? {
			match event {
				tangram_sandbox::stdio::write::Event::End => break,
				tangram_sandbox::stdio::write::Event::Write(length) => {
					*position = position
						.checked_add(length.to_u64().unwrap())
						.ok_or_else(|| tg::error!("the stdin position is too large"))?;
				},
			}
		}

		Ok(*position)
	}

	async fn handle_process_control_stdin_close_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
	) -> tg::Result<()> {
		let input = stream::once(future::ok(tangram_sandbox::stdio::read::Event::End));
		let output = sandbox
			.write_stdio(
				sandbox_process,
				vec![tg::process::stdio::Stream::Stdin],
				input,
			)
			.await
			.inspect_err(|error| {
				tracing::error!(error = %error.trace(), "failed to close the process stdin");
			})?;
		let mut output = pin!(output);
		while let Some(event) = output.try_next().await? {
			if matches!(event, tangram_sandbox::stdio::write::Event::End) {
				break;
			}
		}

		Ok(())
	}
}
