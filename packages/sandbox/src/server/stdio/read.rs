use {
	crate::{Stdio, server::Server},
	futures::{
		Stream, StreamExt as _, future,
		stream::{self},
	},
	std::{
		pin::Pin,
		sync::Arc,
		task::{Context, Poll},
	},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tokio::io::{AsyncRead, ReadBuf},
	tokio_util::io::ReaderStream,
};

enum Reader {
	Null(tokio::io::Empty),
	Pty(Arc<crate::pty::Pty>),
	Stderr(tokio::sync::OwnedMutexGuard<tokio::process::ChildStderr>),
	Stdout(tokio::sync::OwnedMutexGuard<tokio::process::ChildStdout>),
}

impl Server {
	pub async fn read_stdio(
		&self,
		id: tg::process::Id,
		arg: crate::client::stdio::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static>
	{
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		let mut streams = Vec::new();
		for stream in arg.streams {
			if matches!(
				stream,
				tg::process::stdio::Stream::Stdout | tg::process::stdio::Stream::Stderr
			) && !streams.contains(&stream)
			{
				streams.push(stream);
			}
		}
		if streams.is_empty() {
			return Err(tg::error!("expected stdout or stderr for a stdio read"));
		}

		let process = self
			.processes
			.get(&id)
			.ok_or_else(|| tg::error!(process = %id, "not found"))?;
		let stdout_mode = process.command.stdout;
		let stderr_mode = process.command.stderr;
		let stdout = process.stdout.clone();
		let stderr = process.stderr.clone();
		let pty = process.pty.clone();
		drop(process);

		let streams = if streams.contains(&tg::process::stdio::Stream::Stdout)
			&& streams.contains(&tg::process::stdio::Stream::Stderr)
			&& matches!(stdout_mode, Stdio::Tty)
			&& matches!(stderr_mode, Stdio::Tty)
		{
			vec![tg::process::stdio::Stream::Stdout]
		} else {
			streams
		};

		let mut outputs = Vec::with_capacity(streams.len());
		for stream_ in streams {
			let reader = match stream_ {
				tg::process::stdio::Stream::Stdout => match stdout_mode {
					Stdio::Null => Reader::Null(tokio::io::empty()),
					Stdio::Pipe => Reader::Stdout(
						stdout
							.clone()
							.ok_or_else(|| tg::error!(process = %id, "stdout is not available"))?
							.lock_owned()
							.await,
					),
					Stdio::Tty => Reader::Pty(
						pty.clone()
							.ok_or_else(|| tg::error!(process = %id, "stdout is not available"))?,
					),
				},
				tg::process::stdio::Stream::Stderr => match stderr_mode {
					Stdio::Null => Reader::Null(tokio::io::empty()),
					Stdio::Pipe => Reader::Stderr(
						stderr
							.clone()
							.ok_or_else(|| tg::error!(process = %id, "stderr is not available"))?
							.lock_owned()
							.await,
					),
					Stdio::Tty => Reader::Pty(
						pty.clone()
							.ok_or_else(|| tg::error!(process = %id, "stderr is not available"))?,
					),
				},
				tg::process::stdio::Stream::Stdin => unreachable!(),
			};
			let stream =
				ReaderStream::new(reader).map(move |result: std::io::Result<bytes::Bytes>| {
					result
						.map(|bytes| {
							tg::process::stdio::read::Event::Chunk(tg::process::stdio::Chunk {
								bytes,
								position: None,
								stream: stream_,
							})
						})
						.map_err(|error| tg::error!(!error, "failed to read process stdio"))
				});
			outputs.push(stream);
		}

		let stream = stream::select_all(outputs).chain(stream::once(future::ok(
			tg::process::stdio::read::Event::End,
		)));

		Ok(stream)
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

impl AsyncRead for Reader {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		match self.get_mut() {
			Self::Null(reader) => Pin::new(reader).poll_read(cx, buf),
			Self::Pty(pty) => {
				let mut pty = pty.as_ref();
				match Pin::new(&mut pty).poll_read(cx, buf) {
					Poll::Ready(Err(error)) if error.raw_os_error() == Some(libc::EIO) => {
						Poll::Ready(Ok(()))
					},
					poll => poll,
				}
			},
			Self::Stderr(stderr) => Pin::new(&mut **stderr).poll_read(cx, buf),
			Self::Stdout(stdout) => Pin::new(&mut **stdout).poll_read(cx, buf),
		}
	}
}
