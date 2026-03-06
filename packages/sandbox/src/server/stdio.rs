use {
	crate::{
		common::{InputStream, OutputStream},
		server::Server,
	},
	bytes::Bytes,
	futures::TryStreamExt as _,
	tangram_client::prelude::*,
	tangram_futures::{BoxAsyncRead, read::Ext as _},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::io::{ReaderStream, StreamReader},
};

impl Server {
	pub async fn stdin(
		&self,
		arg: crate::client::stdio::StdinArg,
		stdin: BoxAsyncRead<'static>,
	) -> tg::Result<()> {
		let mut stream = ReaderStream::new(stdin);
		while let Some(chunk) = stream
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the stream"))?
		{
			if self
				.processes
				.get(&arg.id)
				.is_some_and(|child| child.status.is_some())
			{
				break;
			}
			let stdio = self
				.stdio
				.get(&arg.id)
				.ok_or_else(|| tg::error!(process = %arg.id, "not found"))?;
			let mut stdin = stdio.stdin.lock().await;
			match &mut *stdin {
				InputStream::Null => break,
				InputStream::Pipe(pipe) => {
					pipe.write_all(&chunk)
						.await
						.map_err(|source| tg::error!(!source, "failed to write stdin"))?;
				},
				InputStream::Pty(pty) => {
					pty.write_all(&chunk)
						.await
						.map_err(|source| tg::error!(!source, "failed to write stdin"))?;
				},
			}
		}
		Ok(())
	}

	pub(crate) async fn handle_stdin(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("missing query params"))?;

		// Get stdin.
		let stdin = request.reader().boxed();

		// Write stdin.
		self.stdin(arg, stdin)
			.await
			.map_err(|source| tg::error!(!source, "failed to handle stdin"))?;

		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	pub async fn stdout(
		&self,
		arg: crate::client::stdio::StdoutArg,
	) -> tg::Result<BoxAsyncRead<'static>> {
		let (sender, receiver) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(1);
		tokio::spawn({
			let server = self.clone();
			async move {
				loop {
					let Some(stdio) = server.stdio.get(&arg.id) else {
						break;
					};
					let mut stdout = stdio.stdout.lock().await;
					match &mut *stdout {
						OutputStream::Null => break,
						OutputStream::Pipe(pipe) => {
							let mut buf = vec![0u8; 1 << 14];
							match pipe.read(&mut buf).await {
								Ok(0) => break,
								Ok(n) => {
									buf.truncate(n);
									if sender.send(Ok(buf.into())).await.is_err() {
										break;
									}
								},
								Err(error) => {
									if sender.send(Err(error)).await.is_err() {
										break;
									}
								},
							}
						},
						OutputStream::Pty(pty) => {
							let mut buf = vec![0u8; 1 << 14];
							match pty.read(&mut buf).await {
								Ok(0) => continue,
								Ok(n) => {
									buf.truncate(n);
									if sender.send(Ok(buf.into())).await.is_err() {
										break;
									}
								},
								Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
								Err(error) => {
									if sender.send(Err(error)).await.is_err() {
										break;
									}
								},
							}
						},
					}
				}
			}
		});
		let reader = StreamReader::new(ReceiverStream::new(receiver));
		Ok(Box::pin(reader))
	}

	pub(crate) async fn handle_stdout(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("missing query params"))?;

		// Get stdout.
		let stdout = self
			.stdout(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to handle stdout"))?;

		let response = http::Response::builder()
			.body(BoxBody::with_reader(stdout))
			.unwrap();
		Ok(response)
	}

	pub async fn stderr(
		&self,
		arg: crate::client::stdio::StderrArg,
	) -> tg::Result<BoxAsyncRead<'static>> {
		let (sender, receiver) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(1);
		tokio::spawn({
			let server = self.clone();
			async move {
				loop {
					let Some(stdio) = server.stdio.get(&arg.id) else {
						break;
					};
					let mut stderr = stdio.stderr.lock().await;
					match &mut *stderr {
						OutputStream::Null => break,
						OutputStream::Pipe(pipe) => {
							let mut buf = vec![0u8; 1 << 14];
							match pipe.read(&mut buf).await {
								Ok(0) => break,
								Ok(n) => {
									buf.truncate(n);
									if sender.send(Ok(buf.into())).await.is_err() {
										break;
									}
								},
								Err(error) => {
									if sender.send(Err(error)).await.is_err() {
										break;
									}
								},
							}
						},
						OutputStream::Pty(pty) => {
							let mut buf = vec![0u8; 1 << 14];
							match pty.read(&mut buf).await {
								Ok(0) => continue,
								Ok(n) => {
									buf.truncate(n);
									if sender.send(Ok(buf.into())).await.is_err() {
										break;
									}
								},
								Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
								Err(error) => {
									if sender.send(Err(error)).await.is_err() {
										break;
									}
								},
							}
						},
					}
				}
			}
		});
		let reader = StreamReader::new(ReceiverStream::new(receiver));
		Ok(Box::pin(reader))
	}

	pub(crate) async fn handle_stderr(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("missing query params"))?;

		// Get stderr.
		let stderr = self
			.stderr(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to handle stderr"))?;

		let response = http::Response::builder()
			.body(BoxBody::with_reader(stderr))
			.unwrap();
		Ok(response)
	}
}
