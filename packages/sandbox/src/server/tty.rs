use {
	crate::server::Server,
	std::os::fd::AsRawFd,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext, builder::Ext as _},
	},
};

impl Server {
	pub async fn set_tty_size(&self, arg: crate::client::tty::SizeArg) -> tg::Result<()> {
		let child = self
			.stdio
			.get(&arg.id)
			.ok_or_else(|| tg::error!(process = %arg.id, "not found"))?;
		let Some(tty) = &child.pty else {
			return Err(
				tg::error!(process = %arg.id, "process does not have a tty associated with it"),
			);
		};
		let tty = tty.lock().await;
		let fd = tty.master.as_raw_fd();
		let size = arg.size;
		unsafe {
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
					return Err(tg::error!(!error, "failed to set the tty size"));
				}
			}
		}
		Ok(())
	}

	pub(crate) async fn handle_set_tty_size_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;

		// Set the tty size.
		self.set_tty_size(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to set the tty size"))?;

		let response = http::Response::builder().empty().unwrap().boxed_body();

		Ok(response)
	}
}
