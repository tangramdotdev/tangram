use {
	crate::{
		common::{InputStream, OutputStream},
		server::Server,
	},
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
	tokio_stream::wrappers::ReceiverStream,
};

enum OutputEvent {
	Chunk(tg::process::stdio::Chunk),
	End,
	Error(tg::Error),
}

impl Server {
	pub async fn read_stdio(
		&self,
		id: tg::process::Id,
		arg: crate::client::stdio::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		let streams = arg
			.streams
			.iter()
			.filter(|stream| {
				matches!(
					stream,
					tg::process::stdio::Stream::Stdout | tg::process::stdio::Stream::Stderr
				)
			})
			.copied()
			.collect::<Vec<_>>();
		if streams.is_empty() {
			return Err(tg::error!("expected stdout or stderr for a stdio read"));
		}
		let streams = if streams.contains(&tg::process::stdio::Stream::Stdout)
			&& streams.contains(&tg::process::stdio::Stream::Stderr)
		{
			if let Some((stdout, stderr)) = match self.processes.get(&id) {
				Some(process) => Some((process.stdout.clone(), process.stderr.clone())),
				None => None,
			} {
				let stdout_is_tty = matches!(&*stdout.lock().await, OutputStream::Pty(_));
				let stderr_is_tty = matches!(&*stderr.lock().await, OutputStream::Pty(_));
				if stdout_is_tty && stderr_is_tty {
					vec![tg::process::stdio::Stream::Stdout]
				} else {
					streams
				}
			} else {
				streams
			}
		} else {
			streams
		};
		let (sender, receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::process::stdio::read::Event>>(1);
		let (output_sender, mut output_receiver) = tokio::sync::mpsc::channel::<OutputEvent>(1);
		let mut stream = ReceiverStream::new(receiver).boxed();
		for stream_ in streams.iter().copied() {
			let task = Task::spawn({
				let server = self.clone();
				let id = id.clone();
				let output_sender = output_sender.clone();
				move |_| async move {
					loop {
						let output = match server.processes.get(&id) {
							Some(process) => match stream_ {
								tg::process::stdio::Stream::Stdout => process.stdout.clone(),
								tg::process::stdio::Stream::Stderr => process.stderr.clone(),
								tg::process::stdio::Stream::Stdin => unreachable!(),
							},
							None => break,
						};
						let result = match stream_ {
							tg::process::stdio::Stream::Stdout
							| tg::process::stdio::Stream::Stderr => {
								let mut output = output.lock().await;
								read_output_chunk(&mut output).await
							},
							tg::process::stdio::Stream::Stdin => {
								unreachable!();
							},
						};
						match result {
							Ok(Some(bytes)) => {
								let event = OutputEvent::Chunk(tg::process::stdio::Chunk {
									bytes,
									position: None,
									stream: stream_,
								});
								if output_sender.send(event).await.is_err() {
									break;
								}
							},
							Ok(None) => {
								output_sender.send(OutputEvent::End).await.ok();
								break;
							},
							Err(error) => {
								output_sender
									.send(OutputEvent::Error(tg::error!(
										!error,
										"failed to read process stdio"
									)))
									.await
									.ok();
								break;
							},
						}
					}
				}
			});
			stream = stream.attach(task).boxed();
		}
		drop(output_sender);
		let task = Task::spawn({
			let total = streams.len();
			move |_| async move {
				let mut ended = 0;
				while let Some(event) = output_receiver.recv().await {
					match event {
						OutputEvent::Chunk(chunk) => {
							if sender
								.send(Ok(tg::process::stdio::read::Event::Chunk(chunk)))
								.await
								.is_err()
							{
								break;
							}
						},
						OutputEvent::End => {
							ended += 1;
							if ended == total {
								sender
									.send(Ok(tg::process::stdio::read::Event::End))
									.await
									.ok();
								break;
							}
						},
						OutputEvent::Error(error) => {
							if sender.send(Err(error)).await.is_err() {
								break;
							}
						},
					}
				}
			}
		});
		Ok(stream.attach(task).boxed())
	}

	pub async fn write_stdio(
		&self,
		id: tg::process::Id,
		arg: crate::client::stdio::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::write::Event>>> {
		if arg.streams.as_slice() != [tg::process::stdio::Stream::Stdin] {
			return Err(tg::error!("expected stdin for a stdio write"));
		}
		let (sender, receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::process::stdio::write::Event>>(1);
		let task =
			Task::spawn({
				let server = self.clone();
				move |_| async move {
					let mut input = pin!(input);
					while let Some(event) = input.next().await {
						let event = match event {
							Ok(event) => event,
							Err(error) => {
								sender.send(Err(error)).await.ok();
								return;
							},
						};
						let stdin = match server.processes.get(&id) {
							Some(process) => process.stdin.clone(),
							None => break,
						};
						let mut stdin = stdin.lock().await;
						match event {
							tg::process::stdio::read::Event::Chunk(chunk) => match &mut *stdin {
								InputStream::Null => break,
								InputStream::Pipe(pipe) => {
									if let Err(error) =
										pipe.write_all(&chunk.bytes).await.map_err(|source| {
											tg::error!(!source, "failed to write stdin")
										}) {
										sender.send(Err(error)).await.ok();
										return;
									}
								},
								InputStream::Pty(pty) => {
									if let Err(error) =
										pty.write_all(&chunk.bytes).await.map_err(|source| {
											tg::error!(!source, "failed to write stdin")
										}) {
										sender.send(Err(error)).await.ok();
										return;
									}
								},
							},
							tg::process::stdio::read::Event::End => break,
						}
					}
					sender
						.send(Ok(tg::process::stdio::write::Event::End))
						.await
						.ok();
				}
			});
		Ok(ReceiverStream::new(receiver).attach(task).boxed())
	}

	pub(crate) async fn handle_read_stdio_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let id: tg::process::Id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		let arg: crate::client::stdio::Arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or(crate::client::stdio::Arg {
				streams: Vec::new(),
			});
		let stream = self
			.read_stdio(id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to handle stdio"))?;
		let stream = stream.map(
			|result: tg::Result<tg::process::stdio::read::Event>| match result {
				Ok(event) => event.try_into(),
				Err(error) => error.try_into(),
			},
		);
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::TEXT_EVENT_STREAM.to_string(),
			)
			.body(BoxBody::with_sse_stream(stream))
			.unwrap();
		Ok(response)
	}

	pub(crate) async fn handle_write_stdio_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let id: tg::process::Id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		let arg: crate::client::stdio::Arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or(crate::client::stdio::Arg {
				streams: Vec::new(),
			});
		let input = request
			.sse()
			.map_err(|source| tg::error!(!source, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			})
			.boxed();
		let output = self
			.write_stdio(id, arg, input)
			.await
			.map_err(|source| tg::error!(!source, "failed to handle stdio"))?;
		let stream = output.map(
			|result: tg::Result<tg::process::stdio::write::Event>| match result {
				Ok(event) => event.try_into(),
				Err(error) => error.try_into(),
			},
		);
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::TEXT_EVENT_STREAM.to_string(),
			)
			.body(BoxBody::with_sse_stream(stream))
			.unwrap();
		Ok(response)
	}
}

async fn read_output_chunk(output: &mut OutputStream) -> std::io::Result<Option<Bytes>> {
	match output {
		OutputStream::Null => Ok(None),
		OutputStream::Pipe(pipe) => {
			let mut buffer = vec![0u8; 1 << 14];
			match pipe.read(&mut buffer).await {
				Ok(0) => Ok(None),
				Ok(n) => {
					buffer.truncate(n);
					Ok(Some(buffer.into()))
				},
				Err(error) => Err(error),
			}
		},
		OutputStream::Pty(pty) => {
			let mut buffer = vec![0u8; 1 << 14];
			match pty.read(&mut buffer).await {
				Ok(0) => Ok(None),
				Ok(n) => {
					buffer.truncate(n);
					Ok(Some(buffer.into()))
				},
				Err(error) if error.raw_os_error() == Some(libc::EIO) => Ok(None),
				Err(error) => Err(error),
			}
		},
	}
}
