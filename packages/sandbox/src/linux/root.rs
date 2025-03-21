use super::{Context, guest, init};
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

		// Get the clone flags.
		let mut flags = 0;
		if context.root.is_some() {
			flags |= libc::CLONE_NEWNS;
			flags |= libc::CLONE_NEWPID;
			flags |= libc::CLONE_NEWUTS;
		}
		if !context.network {
			flags |= libc::CLONE_NEWNET;
		}

		// Spawn the next process.
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
			if context.root.is_some() {
				init::main(context);
			} else {
				guest::main(context);
			}
		}

		// Send the child process's PID to the host process.
		let ret = libc::send(
			context.socket.as_raw_fd(),
			std::ptr::addr_of!(pid).cast(),
			std::mem::size_of_val(&pid),
			0,
		);
		if ret == -1 {
			abort_errno!("send failed");
		}

		// Wait for the child.
		let mut status = 0;
		let ret = libc::waitpid(pid, std::ptr::addr_of_mut!(status), libc::__WALL);
		if ret == -1 {
			abort_errno!("waitpid failed");
		}

		// If there's no init process, send the exit code to the waiting parent.
		// Send the child process's PID to the host process.
		if context.root.is_none() {
			let ret = libc::send(
				context.socket.as_raw_fd(),
				std::ptr::addr_of!(status).cast(),
				std::mem::size_of_val(&status),
				0,
			);
			if ret == -1 {
				abort_errno!("send failed");
			}
		}

		// Exit.
		libc::exit(0);
	}
}
