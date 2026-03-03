use {
	crate::{Context, Server},
	futures::{FutureExt as _, future},
	std::os::fd::AsRawFd,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Server {
	pub async fn try_put_pty_size_with_context(
		&self,
		_context: &Context,
		id: &tg::pty::Id,
		arg: tg::pty::size::put::Arg,
	) -> tg::Result<()> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref()) {
			return self
				.try_put_pty_size_local(id, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to put the pty size"));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		self.try_put_pty_size_remote(id, arg, &remotes)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to put the pty size on the remote"))
	}

	async fn try_put_pty_size_local(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::put::Arg,
	) -> tg::Result<()> {
		// Attempt to set the size of the PTY.
		let pty = self
			.ptys
			.get_mut(id)
			.ok_or_else(|| tg::error!("expected a pty"))?;
		let fd = if arg.master
			&& let Some(master) = pty.master.as_ref()
		{
			master.as_raw_fd()
		} else if let Some(slave) = pty.slave.as_ref() {
			slave.as_raw_fd()
		} else {
			return Ok(());
		};
		let size = arg.size;
		tokio::task::spawn_blocking(move || unsafe {
			let mut winsize = libc::winsize {
				ws_col: size.cols,
				ws_row: size.rows,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			let ret = libc::ioctl(fd, libc::TIOCSWINSZ, std::ptr::addr_of_mut!(winsize));
			if ret != 0 {
				let error = std::io::Error::last_os_error();
				if !matches!(error.raw_os_error(), Some(libc::EBADF)) {
					return Err(error);
				}
			}
			Ok(())
		})
		.await
		.map_err(|source| tg::error!(!source, "the task panicked"))?
		.map_err(|source| tg::error!(!source, "failed to set the pty size"))?;

		Ok(())
	}

	async fn try_put_pty_size_remote(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::put::Arg,
		remotes: &[String],
	) -> tg::Result<()> {
		if remotes.is_empty() {
			return Ok(());
		}
		let arg = tg::pty::size::put::Arg {
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
					.put_pty_size(id, arg)
					.await
					.map_err(|source| tg::error!(!source, %remote, "failed to put the pty size"))
			}
			.boxed()
		});
		future::select_ok(futures).await?;
		Ok(())
	}

	pub(crate) async fn handle_put_pty_size_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
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
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;

		// Put the pty size.
		self.try_put_pty_size_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to put the pty size"))?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().body(BoxBody::empty()).unwrap();
		Ok(response)
	}
}
