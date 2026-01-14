use {
	crate::{Context, Server},
	futures::{Stream, StreamExt as _, future, stream::TryStreamExt as _},
	num::ToPrimitive as _,
	std::{
		os::fd::{AsRawFd as _, RawFd},
		pin::pin,
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Stop},
	tangram_http::{Body, request::Ext as _},
};

impl Server {
	pub async fn write_pty_with_context(
		&self,
		_context: &Context,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
		stream: impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::pty::write::Arg {
				local: None,
				master: arg.master,
				remotes: None,
			};
			return client
				.write_pty(id, arg, stream.boxed())
				.await
				.map_err(|source| tg::error!(!source, "failed to write to the pty on remote"));
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

		let mut stream = pin!(stream);
		while let Some(event) = stream
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to read from the stream"))?
		{
			match event {
				tg::pty::Event::Chunk(chunk) => {
					tokio::task::spawn_blocking(move || unsafe {
						let mut chunk = chunk.as_ref();
						while !chunk.is_empty() {
							let n = libc::write(fd, chunk.as_ptr().cast(), chunk.len());
							if n < 0 {
								let error = std::io::Error::last_os_error();
								return Err(error);
							}
							let n = n.to_usize().unwrap();
							if n == 0 {
								break;
							}
							chunk = &chunk[n..];
						}
						Ok(())
					})
					.await
					.map_err(|source| tg::error!(!source, "the pty write task panicked"))?
					.map_err(|source| tg::error!(!source, "failed to write to the pty"))?;
				},
				tg::pty::Event::Size(size) => {
					Self::pty_write_set_size(fd.as_raw_fd(), size)
						.await
						.map_err(|source| tg::error!(!source, "failed to change the size"))?;
				},
				tg::pty::Event::End => {
					return Err(tg::error!("cannot write an end event"));
				},
			}
		}

		Ok(())
	}

	async fn pty_write_set_size(fd: RawFd, size: tg::pty::Size) -> std::io::Result<()> {
		tokio::task::spawn_blocking(move || unsafe {
			let mut winsize = libc::winsize {
				ws_col: size.cols,
				ws_row: size.rows,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			let ret = libc::ioctl(fd, libc::TIOCSWINSZ, std::ptr::addr_of_mut!(winsize));
			if ret != 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok(())
		})
		.await
		.map_err(std::io::Error::other)?
	}

	pub(crate) async fn handle_write_pty_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the ID.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the pty id"))?;

		// Get the query.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move {
			stop.wait().await;
		};

		// Create the stream.
		let stream = request
			.sse()
			.map(|event| match event {
				Ok(event) => event.try_into(),
				Err(source) => Err(source.into()),
			})
			.take_while_inclusive(|event| future::ready(!matches!(event, Ok(tg::pty::Event::End))))
			.take_until(stop)
			.boxed();

		self.write_pty_with_context(context, &id, arg, stream)
			.await?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::APPLICATION, mime::JSON)) => (),
			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		}

		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.body(Body::empty())
			.unwrap();

		Ok(response)
	}
}
