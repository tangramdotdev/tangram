use {
	crate::{Context, Server},
	bytes::Bytes,
	futures::{FutureExt as _, Stream, StreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::os::fd::AsRawFd as _,
	tangram_client::prelude::*,
	tangram_futures::task::Stop,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn try_read_pty_with_context(
		&self,
		_context: &Context,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + use<>>> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(stream) = self
				.try_read_pty_local(id, arg.clone())
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to read the pty"))?
		{
			return Ok(Some(stream.left_stream()));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(stream) = self
			.try_read_pty_remote(id, arg.clone(), &remotes)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to read the pty from the remote"))?
		{
			return Ok(Some(stream.right_stream()));
		}

		Ok(None)
	}

	async fn try_read_pty_local(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + use<>>> {
		let Some(pty) = self.ptys.get_mut(id) else {
			return Ok(None);
		};
		let fd = if arg.master {
			let Some(master) = pty.master.as_ref() else {
				return Err(tg::error!("the pty master is closed"));
			};
			master.as_raw_fd()
		} else {
			let Some(slave) = pty.slave.as_ref() else {
				return Err(tg::error!("the pty slave is closed"));
			};
			slave.as_raw_fd()
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
			.map_err(|source| tg::error!(!source, "the pty read task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to read the pty"))?
			else {
				return Ok::<_, tg::Error>(None);
			};
			Ok::<_, tg::Error>(Some((bytes, fd)))
		})
		.chain(stream::once(future::ok(tg::pty::Event::End)));

		Ok(Some(stream))
	}

	async fn try_read_pty_remote(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
		remotes: &[String],
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + use<>>> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let arg = tg::pty::read::Arg {
			local: None,
			remotes: None,
			..arg
		};
		let futures = remotes.iter().map(|remote| {
			let remote = remote.clone();
			let arg = arg.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				client
					.try_read_pty(id, arg)
					.await
					.map_err(|source| tg::error!(!source, %remote, "failed to read the pty"))?
					.ok_or_else(|| tg::error!("not found"))
					.map(futures::StreamExt::boxed)
			}
			.boxed()
		});
		let Ok((stream, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		Ok(Some(stream))
	}

	pub(crate) async fn handle_read_pty_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
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

		// Get the stream.
		let Some(stream) = self.try_read_pty_with_context(context, &id, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();

		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
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
