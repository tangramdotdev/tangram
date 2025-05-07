use std::os::fd::AsRawFd;

use crate::Server;
use futures::{Stream, StreamExt as _, future, stream};
use tangram_client as tg;
use tangram_futures::task::Stop;
use tangram_http::{Body, request::Ext as _};
use tokio::io::unix::AsyncFd;

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

		let fd = if arg.master {
			self.ptys
				.get(id)
				.ok_or_else(|| tg::error!("failed to get pty"))?
				.host
				.try_clone()
				.map_err(|source| tg::error!(!source, "failed to clone fd"))?
		} else {
			self.ptys
				.get(id)
				.ok_or_else(|| tg::error!("failed to get pty"))?
				.guest
				.try_clone()
				.map_err(|source| tg::error!(!source, "failed to clone fd"))?
		};

		let fd = AsyncFd::with_interest(fd, tokio::io::Interest::READABLE)
			.map_err(|source| tg::error!(!source, "failed to read pty"))?;

		let stream = stream::try_unfold(fd, |fd| async move {
			let Some(bytes) = fd
				.async_io(tokio::io::Interest::READABLE, |fd| {
					let mut buf = vec![0u8; 4096];
					unsafe {
						let n = libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len());
						if n < 0 {
							let error = std::io::Error::last_os_error();
							if matches!(error.raw_os_error(), Some(libc::EIO)) {
								return Ok(None);
							}
							return Err(error);
						}
						if n == 0 {
							return Ok(None);
						}
						Ok(Some(tg::pty::Event::Chunk(buf.into())))
					}
				})
				.await
				.map_err(|source| tg::error!(!source, "failed to read pty"))?
			else {
				return Ok::<_, tg::Error>(None);
			};
			Ok::<_, tg::Error>(Some((bytes, fd)))
		})
		.chain(stream::once(future::ok(tg::pty::Event::End)))
		.boxed()
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
