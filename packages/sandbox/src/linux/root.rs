use super::{Context, guest};
use crate::common::abort_errno;
use num::ToPrimitive as _;
use std::os::fd::AsRawFd as _;

// The "root" process takes over after the host spawns with CLONE_NEWUSER.
pub fn main(context: Context) -> ! {
	unsafe {
		// If the host process dies, kill this process.
		if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
			abort_errno!("prctl failed");
		}

		// Register signal handlers.
		for signum in FORWARDED_SIGNALS {
			libc::signal(signum, handler as *const () as _);
		}

		// Get the clone flags.
		let mut flags = 0;
		if context.root.is_some() {
			flags |= libc::CLONE_NEWNS;
			flags |= libc::CLONE_NEWUTS;
		}
		if !context.network {
			flags |= libc::CLONE_NEWNET;
		}

		// Spawn the guest process.
		let mut clone_args = libc::clone_args {
			flags: flags.try_into().unwrap(),
			stack: 0,
			stack_size: 0,
			pidfd: 0,
			child_tid: 0,
			parent_tid: 0,
			exit_signal: 0,
			tls: 0,
			set_tid: 0,
			set_tid_size: 0,
			cgroup: 0,
		};
		let pid = libc::syscall(
			libc::SYS_clone3,
			std::ptr::addr_of_mut!(clone_args),
			std::mem::size_of::<libc::clone_args>(),
		);
		let pid = pid.to_i32().unwrap();
		if pid == -1 {
			libc::close(context.socket.as_raw_fd());
			abort_errno!("clone3 failed");
		}

		// If this is the child process, run either init or guest.
		if pid == 0 {
			guest::main(context);
		}

		// Wait for the child.
		let mut status = 0;
		let ret = libc::waitpid(pid, std::ptr::addr_of_mut!(status), libc::__WALL);
		if ret == -1 {
			abort_errno!("waitpid failed");
		}
		if libc::WIFEXITED(status) {
			let status = libc::WEXITSTATUS(status);
			libc::exit(status);
		}

		if libc::WIFSIGNALED(status) {
			let signal = libc::WTERMSIG(status);
			libc::exit(signal + 128);
		}

		// Exit with the same status.
		libc::exit(1);
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
