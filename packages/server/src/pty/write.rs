use {
	crate::{Context, Server},
	num::ToPrimitive as _,
	std::os::fd::AsRawFd as _,
	tangram_client as tg,
	tangram_http::{Body, request::Ext as _},
};

impl Server {
	pub async fn write_pty_with_context(
		&self,
		_context: &Context,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::pty::write::Arg {
				bytes: arg.bytes,
				local: None,
				master: arg.master,
				remotes: None,
			};
			return client
				.write_pty(id, arg)
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

		tokio::task::spawn_blocking(move || unsafe {
			let mut chunk = arg.bytes.as_ref();
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

		Ok(())
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

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		self.write_pty_with_context(context, &id, arg).await?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().body(Body::empty()).unwrap();
		Ok(response)
	}
}
