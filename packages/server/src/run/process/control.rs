use {
	crate::Server,
	bytes::Bytes,
	futures::{
		StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	std::{
		collections::{BTreeMap, BTreeSet, btree_map::Entry},
		pin::pin,
	},
	tangram_client::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

struct Reader {
	buffer: Bytes,
	stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
}

impl Server {
	pub(crate) async fn process_control_task(
		&self,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		let (sender, responses) = tokio::sync::mpsc::channel(512);

		let responses = ReceiverStream::new(responses).boxed();
		let mut requests = self
			.try_get_process_control_stream_all(process, responses)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the control stream"))?
			.ok_or_else(|| tg::error!("expected a control stream"))?
			.boxed();

		let mut readers = BTreeMap::new();
		let mut previous = BTreeSet::new();
		while let Some(event) = requests
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the next control request"))?
		{
			match event {
				tg::process::control::RequestEvent::Request(request) => {
					if !previous.insert(request.id) {
						continue;
					}

					match request.kind {
						tg::process::control::RequestKind::Read(read) => {
							let result = Self::handle_process_control_read_request(
								sandbox,
								sandbox_process,
								&mut readers,
								read,
							)
							.await
							.map(|response| {
								tg::process::control::ResponseEvent::Response(
									tg::process::control::Response {
										id: request.id,
										kind: tg::process::control::ResponseKind::Read(response),
									},
								)
							});
							sender.send(result).await.ok();
						},
						tg::process::control::RequestKind::Write(write) => {
							let result = Self::handle_process_control_write_request(
								sandbox,
								sandbox_process,
								write,
							)
							.await
							.map(|()| {
								tg::process::control::ResponseEvent::Response(
									tg::process::control::Response {
										id: request.id,
										kind: tg::process::control::ResponseKind::Write,
									},
								)
							});
							sender.send(result).await.ok();
						},
						tg::process::control::RequestKind::Signal(signal) => {
							let result = Self::handle_process_control_signal_request(
								sandbox,
								sandbox_process,
								signal,
							)
							.await
							.map(|()| {
								tg::process::control::ResponseEvent::Response(
									tg::process::control::Response {
										id: request.id,
										kind: tg::process::control::ResponseKind::Signal,
									},
								)
							});
							sender.send(result).await.ok();
						},
						tg::process::control::RequestKind::Tty(tty) => {
							let result = Self::handle_process_control_tty_request(
								sandbox,
								sandbox_process,
								tty,
							)
							.await
							.map(|()| {
								tg::process::control::ResponseEvent::Response(
									tg::process::control::Response {
										id: request.id,
										kind: tg::process::control::ResponseKind::Tty,
									},
								)
							});
							sender.send(result).await.ok();
						},
					}
				},
				tg::process::control::RequestEvent::Stop => break,
			}
		}

		sender
			.send(Ok(tg::process::control::ResponseEvent::Stop))
			.await
			.ok();
		Ok(())
	}

	async fn handle_process_control_read_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		readers: &mut BTreeMap<tg::process::stdio::Stream, Reader>,
		read: tg::process::control::ReadRequest,
	) -> tg::Result<tg::process::control::ReadResponse> {
		// Get or create the reader for the stream.
		let reader = match readers.entry(read.stream) {
			Entry::Occupied(entry) => entry.into_mut(),
			Entry::Vacant(entry) => {
				let stream = sandbox
					.read_stdio(sandbox_process, vec![read.stream])
					.await
					.map_err(|error| tg::error!(!error, "failed to read the process stdio"))?
					.boxed();
				entry.insert(Reader {
					buffer: Bytes::new(),
					stream,
				})
			},
		};

		// Fill the buffer if it is empty. An empty buffer after the stream ends indicates end of file.
		while reader.buffer.is_empty() {
			match reader.stream.try_next().await? {
				Some(tg::process::stdio::read::Event::Chunk(chunk)) => {
					reader.buffer = chunk.bytes;
				},
				Some(tg::process::stdio::read::Event::End) | None => break,
			}
		}

		// Take up to the requested length from the buffer.
		let len = read.len.min(reader.buffer.len());
		let bytes = reader.buffer.split_to(len);

		Ok(tg::process::control::ReadResponse {
			stream: read.stream,
			bytes,
		})
	}

	async fn handle_process_control_write_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		write: tg::process::control::WriteRequest,
	) -> tg::Result<()> {
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
		let mut output = pin!(output);
		while let Some(event) = output.try_next().await? {
			match event {
				tg::process::stdio::write::Event::End => {
					break;
				},
				tg::process::stdio::write::Event::Stop => (),
			}
		}
		Ok(())
	}

	async fn handle_process_control_signal_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		signal: tg::process::control::SignalRequest,
	) -> tg::Result<()> {
		sandbox
			.kill(sandbox_process, signal.signal)
			.await
			.map_err(|error| tg::error!(!error, "failed to signal the process"))?;
		Ok(())
	}

	async fn handle_process_control_tty_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		tty: tg::process::control::TtyRequest,
	) -> tg::Result<()> {
		sandbox
			.set_tty_size(sandbox_process, tty.size)
			.await
			.map_err(|error| tg::error!(!error, "failed to set the tty size"))?;
		Ok(())
	}
}
