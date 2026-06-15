use {
	crate::{Stdio, server::Server},
	futures::{
		Stream, StreamExt as _, TryStreamExt as _, future,
		stream::{self},
	},
	std::{
		pin::Pin,
		sync::Arc,
		task::{Context, Poll},
	},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tokio::io::{AsyncWrite, AsyncWriteExt as _},
};

enum Writer {
	Null(tokio::io::Sink),
	Pty(Arc<crate::pty::Pty>),
	Stdin(tokio::sync::OwnedMutexGuard<tokio::process::ChildStdin>),
}

impl Server {
	pub async fn write_stdio(
		&self,
		id: tg::process::Id,
		arg: crate::client::stdio::Arg,
		stream: impl Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::stdio::write::Event>> + Send + 'static>
	{
		if arg.streams.as_slice() != [tg::process::stdio::Stream::Stdin] {
			return Err(tg::error!("expected stdin for a stdio write"));
		}

		let process = self
			.processes
			.get(&id)
			.ok_or_else(|| tg::error!(process = %id, "failed to find the process"))?;
		let stdin_mode = process.command.stdin;
		let stdin = process.stdin.clone();
		let pty = process.pty.clone();
		drop(process);

		let writer = match stdin_mode {
			Stdio::Null => {
				let writer = tokio::io::sink();
				Writer::Null(writer)
			},
			Stdio::Pipe => {
				let writer = stdin
					.ok_or_else(|| tg::error!(process = %id, "stdin is not available"))?
					.lock_owned()
					.await;
				Writer::Stdin(writer)
			},
			Stdio::Tty => {
				let writer =
					pty.ok_or_else(|| tg::error!(process = %id, "stdin is not available"))?;
				Writer::Pty(writer)
			},
		};

		// For each chunk written, emit a write event with the length of the chunk. A
		// broken pipe indicates that the process closed its stdin, so it is treated as
		// EOF rather than an error.
		let server = self.clone();
		let stream = stream.boxed();
		let output = stream::try_unfold(
			(stream, Some(writer), false),
			move |(mut stream, mut writer, done)| {
				let server = server.clone();
				let id = id.clone();
				async move {
					if done {
						return Ok(None);
					}
					loop {
						let Some(event) = stream
							.try_next()
							.await
							.map_err(|error| tg::error!(!error, "failed to read a stdio event"))?
						else {
							let event = tg::process::stdio::write::Event::End;
							return Ok(Some((event, (stream, writer, true))));
						};
						match event {
							tg::process::stdio::read::Event::Chunk(chunk) => {
								if chunk.bytes.is_empty() {
									continue;
								}
								let result = writer
									.as_mut()
									.ok_or_else(|| tg::error!("stdin was closed"))?
									.write_all(&chunk.bytes)
									.await;
								match result {
									Ok(()) => {
										let event = tg::process::stdio::write::Event::Write(
											chunk.bytes.len(),
										);
										return Ok(Some((event, (stream, writer, false))));
									},
									Err(error)
										if error.kind() == std::io::ErrorKind::BrokenPipe =>
									{
										let event = tg::process::stdio::write::Event::End;
										return Ok(Some((event, (stream, writer, true))));
									},
									Err(error) => {
										return Err(tg::error!(!error, "failed to write stdin"));
									},
								}
							},
							tg::process::stdio::read::Event::End => {
								// Close stdin by dropping the writer and removing the process's stdin, so that the child observes EOF.
								if let Some(mut w) = writer.take() {
									w.shutdown().await.map_err(|error| {
										tg::error!(!error, "failed to close stdin")
									})?;
									let stdin = matches!(w, Writer::Stdin(_));
									drop(w);
									if stdin
										&& let Some(mut process) = server.processes.get_mut(&id)
									{
										process.stdin.take();
									}
								}
								let event = tg::process::stdio::write::Event::End;
								return Ok(Some((event, (stream, writer, true))));
							},
						}
					}
				}
			},
		);

		Ok(output)
	}

	pub(crate) async fn handle_write_stdio_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let id: tg::process::Id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;
		let arg: crate::client::stdio::Arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or(crate::client::stdio::Arg {
				streams: Vec::new(),
			});
		let stream = request
			.sse()
			.map_err(|error| tg::error!(!error, "failed to read an event"))
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
			.write_stdio(id, arg, stream)
			.await
			.map_err(|error| tg::error!(!error, "failed to handle stdio"))?;
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

impl AsyncWrite for Writer {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		match self.get_mut() {
			Self::Null(writer) => Pin::new(writer).poll_write(cx, buf),
			Self::Pty(pty) => {
				let mut pty = pty.as_ref();
				Pin::new(&mut pty).poll_write(cx, buf)
			},
			Self::Stdin(stdin) => Pin::new(&mut **stdin).poll_write(cx, buf),
		}
	}

	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		match self.get_mut() {
			Self::Null(writer) => Pin::new(writer).poll_flush(cx),
			Self::Pty(pty) => {
				let mut pty = pty.as_ref();
				Pin::new(&mut pty).poll_flush(cx)
			},
			Self::Stdin(stdin) => Pin::new(&mut **stdin).poll_flush(cx),
		}
	}

	fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		match self.get_mut() {
			Self::Null(writer) => Pin::new(writer).poll_shutdown(cx),
			Self::Pty(pty) => {
				let mut pty = pty.as_ref();
				Pin::new(&mut pty).poll_shutdown(cx)
			},
			Self::Stdin(stdin) => Pin::new(&mut **stdin).poll_shutdown(cx),
		}
	}
}
