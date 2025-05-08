use super::Context;
use crate::{abort_errno, linux::guest};
use std::os::fd::AsRawFd;

pub fn main(context: Context) -> ! {
	unsafe {
		// Forward signals.
		for signum in FORWARDED_SIGNALS {
			libc::signal(signum, handler as *const () as _);
		}

		// Fork.
		let pid = libc::fork();
		if pid < 0 {
			abort_errno!("fork() failed");
		}

		// Run the guest.
		if pid == 0 {
			guest::main(context);
		}

		// Close unused fds.
		for fd in [context.stdin, context.stdout, context.stderr] {
			libc::close(fd);
		}

		// Reap child processes until the child exits.
		let status = loop {
			let mut status = 0;
			let pid_ = libc::wait(std::ptr::addr_of_mut!(status));
			if pid_ < 0 {
				abort_errno!("wait() failed");
			}
			if pid_ == pid {
				break status;
			}
		};

		// Send the status to the host.
		let ret = libc::send(
			context.socket.as_raw_fd(),
			std::ptr::addr_of!(status).cast(),
			std::mem::size_of_val(&status),
			0,
		);
		if ret == -1 {
			abort_errno!("send failed");
		}

		libc::exit(0);
	}
}

// We forward all signals except SIGCHILD.
const FORWARDED_SIGNALS: [libc::c_int; 29] = [
	libc::SIGINT,
	libc::SIGQUIT,
	libc::SIGILL,
	libc::SIGTRAP,
	libc::SIGABRT,
	libc::SIGBUS,
	libc::SIGFPE,
	libc::SIGKILL,
	libc::SIGUSR1,
	libc::SIGSEGV,
	libc::SIGUSR2,
	libc::SIGPIPE,
	libc::SIGALRM,
	libc::SIGTERM,
	libc::SIGSTKFLT,
	libc::SIGCONT,
	libc::SIGSTOP,
	libc::SIGTSTP,
	libc::SIGTTIN,
	libc::SIGTTOU,
	libc::SIGURG,
	libc::SIGXCPU,
	libc::SIGXFSZ,
	libc::SIGVTALRM,
	libc::SIGPROF,
	libc::SIGWINCH,
	libc::SIGPOLL,
	libc::SIGPWR,
	libc::SIGSYS,
];

unsafe extern "C" fn handler(signal: libc::c_int) {
	unsafe { libc::kill(-1, signal) };
}
