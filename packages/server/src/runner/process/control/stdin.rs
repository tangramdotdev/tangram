use {
	crate::{process::control::local::Reply, session::Session},
	futures::{TryFutureExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::{pin::pin, sync::Arc},
	tangram_client::{
		prelude::*,
		process::stdio::{Chunk, Stream, write::Data},
	},
	tangram_futures::task::{Stopper, Task},
};

pub(super) struct RunProcessControlStdinTaskArg {
	pub(super) exited: Stopper,
	pub(super) receiver:
		tokio::sync::mpsc::Receiver<(String, tg::process::control::WriteServerRequestArg, Reply)>,
	pub(super) sandbox: tangram_sandbox::Sandbox,
	pub(super) sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub(super) stdin: tg::process::Stdio,
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
			exited,
			mut receiver,
			sandbox,
			mut sandbox_process,
			stdin,
		} = arg;
		let sandbox_process = sandbox_process
			.wait_for(Option::is_some)
			.await
			.ok()
			.and_then(|sandbox_process| sandbox_process.as_ref().cloned());
		if !matches!(stdin, tg::process::Stdio::Pipe | tg::process::Stdio::Tty) {
			return Ok(());
		}
		let mut closed = false;
		let mut position = 0_u64;
		while let Some((id, request, sender)) = receiver.recv().await {
			let request = match Self::process_control_stdin_chunk(request) {
				Ok(chunk) => chunk,
				Err(error) => {
					let response = Self::process_control_response(id, Err(error));
					sender.send_low(response).await?;
					continue;
				},
			};

			crate::checkpoint!(
				self.server,
				"runner.process.control.stdin.write",
				close = %request.bytes.is_empty(),
			)
			.await;

			// Once the stdin is unavailable, report it closed without advancing the position.
			let result = if closed || exited.stopped() {
				Self::handle_closed_process_stdin_write_request(&request, position)
			} else if let Some(sandbox_process) = &sandbox_process {
				tokio::select! {
					biased;
					() = exited.wait() => {
						Self::handle_closed_process_stdin_write_request(&request, position)
					},
					result = Self::handle_process_control_stdin_write_request(
						&sandbox,
						sandbox_process,
						&request,
						&mut position,
						&mut closed,
					) => result,
				}
			} else {
				Self::handle_closed_process_stdin_write_request(&request, position)
			};
			let response = result.map(tg::process::control::ClientResponseOutput::Write);
			let response = Self::process_control_response(id, response);
			sender.send_low(response).await?;
		}

		Ok(())
	}

	fn process_control_stdin_chunk(
		data: tg::process::stdio::write::Data,
	) -> tg::Result<tg::process::stdio::Chunk> {
		let chunk = match data {
			Data::Chunk(chunk) => {
				if chunk.bytes.is_empty()
					|| chunk.bytes.len() > tg::process::stdio::flow::CHUNK_SIZE
				{
					return Err(tg::error!("invalid process stdin chunk size"));
				}
				chunk
			},
			Data::End(end) => {
				let position = end
					.stream_positions
					.get(&Stream::Stdin)
					.copied()
					.ok_or_else(|| tg::error!("missing the stdin end position"))?;
				if end.stream_positions.len() != 1 || end.combined_position != position {
					return Err(tg::error!("invalid stdin end positions"));
				}
				Chunk {
					bytes: bytes::Bytes::new(),
					combined_position: position,
					stream: Stream::Stdin,
					stream_position: position,
					timestamp: None,
				}
			},
		};
		if chunk.stream != Stream::Stdin {
			return Err(tg::error!("invalid process stdio stream"));
		}
		Ok(chunk)
	}

	fn handle_closed_process_stdin_write_request(
		request: &tg::process::stdio::Chunk,
		position: u64,
	) -> tg::Result<tg::process::control::WriteClientResponseOutput> {
		let chunk = request;
		if chunk.stream != tg::process::stdio::Stream::Stdin {
			return Err(tg::error!("invalid process stdio stream"));
		}
		let start = chunk.stream_position;
		if start > position {
			return Err(tg::error!(
				expected = %position,
				actual = %start,
				"encountered a gap in the stdin stream"
			));
		}
		let output = tg::process::control::WriteClientResponseOutput {
			closed: true,
			length: position
				.saturating_sub(start)
				.min(chunk.bytes.len().to_u64().unwrap()),
		};

		Ok(output)
	}

	async fn handle_process_control_stdin_write_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		request: &tg::process::stdio::Chunk,
		position: &mut u64,
		closed: &mut bool,
	) -> tg::Result<tg::process::control::WriteClientResponseOutput> {
		let mut chunk = request.clone();
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
			let output = tg::process::control::WriteClientResponseOutput {
				closed: true,
				length: 0,
			};

			return Ok(output);
		}
		if end <= *position {
			let output = tg::process::control::WriteClientResponseOutput {
				closed: false,
				length: chunk.bytes.len().to_u64().unwrap(),
			};

			return Ok(output);
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
		if *position < end {
			*closed = true;
		}
		let output = tg::process::control::WriteClientResponseOutput {
			closed: *closed,
			length: position.saturating_sub(start).min(end - start),
		};

		Ok(output)
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
			.map_err(|error| tg::error!(!error, "failed to close the process stdin"))
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
