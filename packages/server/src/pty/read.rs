use {
	crate::Server,
	bytes::Bytes,
	futures::{Stream, StreamExt as _, future, stream},
	num::ToPrimitive,
	std::os::fd::AsRawFd as _,
	tangram_client as tg,
	tangram_futures::task::Stop,
	tangram_http::{Body, request::Ext as _},
};

impl Server {
	pub async fn read_pty(
		&self,
		id: &tg::pty::Id,
		mut arg: tg::pty::read::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let stream = remote.read_pty(id, arg).await?.left_stream();
			return Ok(stream);
		}

		let pty = self
			.ptys
			.get_mut(id)
			.ok_or_else(|| tg::error!("failed to get the pty"))?;
		let fd = if arg.master {
			pty.master
				.as_ref()
				.ok_or_else(|| tg::error!("the pty master is closed"))?
				.as_raw_fd()
		} else {
			pty.slave
				.as_ref()
				.ok_or_else(|| tg::error!("the pty slave is closed"))?
				.as_raw_fd()
		};
		drop(pty);

		let stream = stream::try_unfold(fd, |fd| async move {
			let Some(bytes) = tokio::task::spawn_blocking(move || unsafe {
				let mut buffer = vec![0u8; 4096];
				let n = libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len());
				if n < 0 {
					let error = std::io::Error::last_os_error();
					#[cfg(target_os = "linux")]
					{
						if error.raw_os_error() == Some(libc::EIO) {
							return Ok(None);
						}
					}
					return Err(error);
				}
				if n == 0 {
					return Ok(None);
				}
				let n = n.to_usize().unwrap();
				let bytes = Bytes::copy_from_slice(&buffer[..n]);
				Ok(Some(tg::pty::Event::Chunk(bytes)))
			})
			.await
			.unwrap()
			.map_err(|source| tg::error!(!source, "failed to read the pty"))?
			else {
				return Ok::<_, tg::Error>(None);
			};
			Ok::<_, tg::Error>(Some((bytes, fd)))
		})
		.chain(stream::once(future::ok(tg::pty::Event::End)))
		.right_stream();

		Ok(stream)
	}

	pub(crate) async fn handle_read_pty_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the stream.
		let stream = handle.read_pty(&id, arg).await?;

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();

		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},
			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
