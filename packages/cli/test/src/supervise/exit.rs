use {
	crate::{Result, process},
	std::{
		os::fd::AsRawFd as _,
		time::{Duration, Instant},
	},
};

/// Wait for a process that is not a child of this process to exit, without polling.
#[derive(clap::Args)]
pub struct Args {
	/// The process to wait for.
	#[arg(long)]
	pid: libc::pid_t,

	/// The number of seconds to wait.
	#[arg(default_value_t = 10, long)]
	timeout: u64,
}

pub fn exit(args: &Args) -> Result<u8> {
	let Some(fd) = process::watch(args.pid)? else {
		return Ok(0);
	};
	let deadline = Instant::now() + Duration::from_secs(args.timeout);
	loop {
		let mut fds = [libc::pollfd {
			events: libc::POLLIN,
			fd: fd.as_raw_fd(),
			revents: 0,
		}];
		process::poll(&mut fds, Some(deadline))?;
		if fds[0].revents != 0 {
			return Ok(0);
		}
		if Instant::now() >= deadline {
			return Err(format!(
				"the process {} did not exit within {} seconds",
				args.pid, args.timeout
			)
			.into());
		}
	}
}
