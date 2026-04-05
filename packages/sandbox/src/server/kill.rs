use {
	crate::server::Server,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext, builder::Ext as _},
	},
};

impl Server {
	pub async fn kill(&self, id: tg::process::Id, arg: crate::client::kill::Arg) -> tg::Result<()> {
		let child = self
			.processes
			.get(&id)
			.ok_or_else(|| tg::error!(process = %id, "not found"))?;
		let signal = match arg.signal {
			tg::process::Signal::SIGABRT => libc::SIGABRT,
			tg::process::Signal::SIGALRM => libc::SIGALRM,
			tg::process::Signal::SIGFPE => libc::SIGFPE,
			tg::process::Signal::SIGHUP => libc::SIGHUP,
			tg::process::Signal::SIGILL => libc::SIGILL,
			tg::process::Signal::SIGINT => libc::SIGINT,
			tg::process::Signal::SIGKILL => libc::SIGKILL,
			tg::process::Signal::SIGPIPE => libc::SIGPIPE,
			tg::process::Signal::SIGQUIT => libc::SIGQUIT,
			tg::process::Signal::SIGSEGV => libc::SIGSEGV,
			tg::process::Signal::SIGTERM => libc::SIGTERM,
			tg::process::Signal::SIGUSR1 => libc::SIGUSR1,
			tg::process::Signal::SIGUSR2 => libc::SIGUSR2,
		};
		let pid = child.pid;
		unsafe {
			let result = libc::kill(pid, signal);
			if result != 0 {
				let error = std::io::Error::last_os_error();
				return Err(
					tg::error!(!error, process = %id, signal = %arg.signal, "failed to signal process"),
				);
			}
		}
		Ok(())
	}

	pub(crate) async fn handle_kill_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let id: tg::process::Id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;

		// Kill.
		self.kill(id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to kill"))?;

		let response = http::Response::builder().empty().unwrap().boxed_body();

		Ok(response)
	}
}
