use {
	crate::{Context, Server},
	futures::{FutureExt as _, future},
	std::os::fd::AsRawFd,
	tangram_client::prelude::*,
	tangram_http::request::Ext as _,
};

impl Server {
	pub async fn try_get_pty_size_with_context(
		&self,
		_context: &Context,
		id: &tg::pty::Id,
		arg: tg::pty::size::get::Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(size) = self
				.try_get_pty_size_local(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the pty size"))?
		{
			return Ok(Some(size));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(size) = self
			.try_get_pty_size_remote(id, arg.clone(), &remotes)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to get the pty size from the remote"),
			)? {
			return Ok(Some(size));
		}

		Ok(None)
	}

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

	async fn try_get_pty_size_local(&self, id: &tg::pty::Id) -> tg::Result<Option<tg::pty::Size>> {
		let Some(pty) = self.ptys.get(id) else {
			return Ok(None);
		};
		let Some(fd) = pty.master.as_ref() else {
			return Ok(None);
		};
		let fd = fd.as_raw_fd();
		let size = tokio::task::spawn_blocking(move || unsafe {
			let mut winsize = std::mem::zeroed::<libc::winsize>();
			let ret = libc::ioctl(fd, libc::TIOCGWINSZ, std::ptr::addr_of_mut!(winsize));
			if ret != 0 {
				let error = std::io::Error::last_os_error();
				if !matches!(error.raw_os_error(), Some(libc::EBADF)) {
					return Err(error);
				}
			}
			let size = tg::pty::Size {
				rows: winsize.ws_row,
				cols: winsize.ws_col,
			};
			Ok(size)
		})
		.await
		.map_err(|source| tg::error!(!source, "the task panicked"))?
		.map_err(|source| tg::error!(!source, "failed to set the pty size"))?;
		Ok(Some(size))
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

	async fn try_get_pty_size_remote(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::get::Arg,
		remotes: &[String],
	) -> tg::Result<Option<tg::pty::Size>> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let arg = tg::pty::size::get::Arg {
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
					.get_pty_size(id, arg)
					.await
					.map_err(|source| tg::error!(!source, %remote, "failed to get the pty size"))
			}
			.boxed()
		});
		let Ok((size, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		Ok(size)
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

	pub(crate) async fn handle_get_pty_size_request(
		&self,
		request: tangram_http::Request,
		context: &Context,
		id: &str,
	) -> tg::Result<tangram_http::Response> {
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

		// Get the pty size.
		let output = self
			.try_get_pty_size_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the pty size"))?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(
					Some(content_type),
					tangram_http::body::Boxed::with_bytes(body),
				)
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();
		Ok(response)
	}

	pub(crate) async fn handle_put_pty_size_request(
		&self,
		request: tangram_http::Request,
		context: &Context,
		id: &str,
	) -> tg::Result<tangram_http::Response> {
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

		let response = http::Response::builder()
			.body(tangram_http::body::Boxed::empty())
			.unwrap();
		Ok(response)
	}
}
