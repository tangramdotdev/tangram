use {
	crate::{common::OutputStream, server::Server},
	bytes::Bytes,
	futures::{StreamExt as _, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tokio::io::AsyncReadExt as _,
	tokio_stream::wrappers::ReceiverStream,
};

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
		let (output_sender, mut output_receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::process::stdio::read::Event>>(1);
		let mut stream = ReceiverStream::new(receiver).boxed();
		for stream_ in streams.iter().copied() {
			let output = match self.processes.get(&id) {
				Some(process) => match stream_ {
					tg::process::stdio::Stream::Stdout => process.stdout.clone(),
					tg::process::stdio::Stream::Stderr => process.stderr.clone(),
					tg::process::stdio::Stream::Stdin => unreachable!(),
				},
				None => return Err(tg::error!(process = %id, "not found")),
			};
			let task = Task::spawn({
				let output = output.clone();
				let output_sender = output_sender.clone();
				move |_| async move {
					loop {
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
								let event = Ok(tg::process::stdio::read::Event::Chunk(
									tg::process::stdio::Chunk {
										bytes,
										position: None,
										stream: stream_,
									},
								));
								if output_sender.send(event).await.is_err() {
									break;
								}
							},
							Ok(None) => {
								output_sender
									.send(Ok(tg::process::stdio::read::Event::End))
									.await
									.ok();
								break;
							},
							Err(error) => {
								output_sender
									.send(Err(tg::error!(!error, "failed to read process stdio")))
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
						Ok(tg::process::stdio::read::Event::Chunk(chunk)) => {
							if sender
								.send(Ok(tg::process::stdio::read::Event::Chunk(chunk)))
								.await
								.is_err()
							{
								break;
							}
						},
						Ok(tg::process::stdio::read::Event::End) => {
							ended += 1;
							if ended == total {
								sender
									.send(Ok(tg::process::stdio::read::Event::End))
									.await
									.ok();
								break;
							}
						},
						Err(error) => {
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
