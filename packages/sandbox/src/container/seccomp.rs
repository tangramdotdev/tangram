use {crate::SeccompPolicy, tangram_client::prelude::*};

const SECCOMP_DATA_ARCH_OFFSET: u32 = 4;
const SECCOMP_DATA_ARGUMENTS_OFFSET: u32 = 16;
const SECCOMP_DATA_NUMBER_OFFSET: u32 = 0;

const CLONE_NAMESPACE_FLAGS: u32 = libc::CLONE_NEWCGROUP as u32
	| libc::CLONE_NEWIPC as u32
	| libc::CLONE_NEWNET as u32
	| libc::CLONE_NEWNS as u32
	| libc::CLONE_NEWPID as u32
	| libc::CLONE_NEWTIME as u32
	| libc::CLONE_NEWUSER as u32
	| libc::CLONE_NEWUTS as u32;

pub fn install(policy: SeccompPolicy) -> tg::Result<()> {
	let mut filter = match policy {
		SeccompPolicy::Default => default_filter()?,
	};
	let length = filter
		.len()
		.try_into()
		.map_err(|error| tg::error!(!error, "the seccomp filter is too large"))?;
	let program = libc::sock_fprog {
		filter: filter.as_mut_ptr(),
		len: length,
	};
	let result = unsafe {
		libc::syscall(
			libc::SYS_seccomp,
			libc::SECCOMP_SET_MODE_FILTER,
			0,
			&raw const program,
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to install the seccomp filter"));
	}
	Ok(())
}

fn default_filter() -> tg::Result<Vec<libc::sock_filter>> {
	let architecture = audit_architecture()?;
	let mut filter = vec![
		statement(
			libc::BPF_LD | libc::BPF_W | libc::BPF_ABS,
			SECCOMP_DATA_ARCH_OFFSET,
		),
		jump(
			libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K,
			architecture,
			1,
			0,
		),
		statement(libc::BPF_RET | libc::BPF_K, libc::SECCOMP_RET_KILL_PROCESS),
		statement(
			libc::BPF_LD | libc::BPF_W | libc::BPF_ABS,
			SECCOMP_DATA_NUMBER_OFFSET,
		),
	];

	#[cfg(target_arch = "x86_64")]
	filter.extend([
		jump(
			libc::BPF_JMP | libc::BPF_JSET | libc::BPF_K,
			0x4000_0000,
			0,
			1,
		),
		statement(libc::BPF_RET | libc::BPF_K, libc::SECCOMP_RET_KILL_PROCESS),
	]);

	// Report clone3 as unavailable so libc can fall back to clone, whose namespace flags are filtered below.
	filter.extend([
		jump(
			libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K,
			syscall_number(libc::SYS_clone3),
			0,
			1,
		),
		statement(
			libc::BPF_RET | libc::BPF_K,
			libc::SECCOMP_RET_ERRNO | libc::ENOSYS as u32,
		),
		jump(
			libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K,
			syscall_number(libc::SYS_clone),
			0,
			5,
		),
		statement(
			libc::BPF_LD | libc::BPF_W | libc::BPF_ABS,
			SECCOMP_DATA_ARGUMENTS_OFFSET,
		),
		statement(
			libc::BPF_ALU | libc::BPF_AND | libc::BPF_K,
			CLONE_NAMESPACE_FLAGS,
		),
		jump(libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K, 0, 1, 0),
		statement(
			libc::BPF_RET | libc::BPF_K,
			libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
		),
		statement(libc::BPF_RET | libc::BPF_K, libc::SECCOMP_RET_ALLOW),
	]);

	// Allow only the personality values used to query or select standard Linux execution domains.
	filter.push(jump(
		libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K,
		syscall_number(libc::SYS_personality),
		0,
		12,
	));
	filter.push(statement(
		libc::BPF_LD | libc::BPF_W | libc::BPF_ABS,
		SECCOMP_DATA_ARGUMENTS_OFFSET,
	));
	for personality in [0, 8, 0x20000, 0x20008, u32::MAX] {
		filter.extend([
			jump(
				libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K,
				personality,
				0,
				1,
			),
			statement(libc::BPF_RET | libc::BPF_K, libc::SECCOMP_RET_ALLOW),
		]);
	}
	filter.push(statement(
		libc::BPF_RET | libc::BPF_K,
		libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
	));

	// Exclude socket families that expose kernel crypto or host virtual sockets.
	filter.extend([
		jump(
			libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K,
			syscall_number(libc::SYS_socket),
			0,
			6,
		),
		statement(
			libc::BPF_LD | libc::BPF_W | libc::BPF_ABS,
			SECCOMP_DATA_ARGUMENTS_OFFSET,
		),
		jump(
			libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K,
			libc::AF_ALG as u32,
			0,
			1,
		),
		statement(
			libc::BPF_RET | libc::BPF_K,
			libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
		),
		jump(
			libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K,
			libc::AF_VSOCK as u32,
			0,
			1,
		),
		statement(
			libc::BPF_RET | libc::BPF_K,
			libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
		),
		statement(libc::BPF_RET | libc::BPF_K, libc::SECCOMP_RET_ALLOW),
	]);

	for syscall in allowed_syscalls() {
		filter.extend([
			jump(libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K, syscall, 0, 1),
			statement(libc::BPF_RET | libc::BPF_K, libc::SECCOMP_RET_ALLOW),
		]);
	}

	filter.push(statement(
		libc::BPF_RET | libc::BPF_K,
		libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
	));

	Ok(filter)
}

#[cfg(any(
	target_arch = "aarch64",
	target_arch = "riscv64",
	target_arch = "x86_64"
))]
fn allowed_syscalls() -> Vec<u32> {
	let syscalls = [
		libc::SYS_accept,
		libc::SYS_accept4,
		libc::SYS_bind,
		libc::SYS_brk,
		libc::SYS_capget,
		libc::SYS_capset,
		libc::SYS_chdir,
		libc::SYS_clock_adjtime,
		libc::SYS_clock_getres,
		libc::SYS_clock_gettime,
		libc::SYS_clock_nanosleep,
		libc::SYS_close,
		libc::SYS_close_range,
		libc::SYS_connect,
		libc::SYS_copy_file_range,
		libc::SYS_dup,
		libc::SYS_dup3,
		libc::SYS_epoll_create1,
		libc::SYS_epoll_ctl,
		libc::SYS_epoll_pwait,
		libc::SYS_epoll_pwait2,
		libc::SYS_eventfd2,
		libc::SYS_execve,
		libc::SYS_execveat,
		libc::SYS_exit,
		libc::SYS_exit_group,
		libc::SYS_faccessat,
		libc::SYS_faccessat2,
		libc::SYS_fadvise64,
		libc::SYS_fallocate,
		libc::SYS_fchdir,
		libc::SYS_fchmod,
		libc::SYS_fchmodat,
		libc::SYS_fchown,
		libc::SYS_fchownat,
		libc::SYS_fcntl,
		libc::SYS_fdatasync,
		libc::SYS_fgetxattr,
		libc::SYS_flistxattr,
		libc::SYS_flock,
		libc::SYS_fremovexattr,
		libc::SYS_fsetxattr,
		libc::SYS_fstat,
		libc::SYS_fstatfs,
		libc::SYS_fsync,
		libc::SYS_ftruncate,
		libc::SYS_futex,
		libc::SYS_futex_waitv,
		libc::SYS_getcpu,
		libc::SYS_getcwd,
		libc::SYS_getdents64,
		libc::SYS_getegid,
		libc::SYS_geteuid,
		libc::SYS_getgid,
		libc::SYS_getgroups,
		libc::SYS_getitimer,
		libc::SYS_getpeername,
		libc::SYS_getpgid,
		libc::SYS_getpid,
		libc::SYS_getppid,
		libc::SYS_getpriority,
		libc::SYS_getrandom,
		libc::SYS_getresgid,
		libc::SYS_getresuid,
		libc::SYS_getrlimit,
		libc::SYS_get_robust_list,
		libc::SYS_getrusage,
		libc::SYS_getsid,
		libc::SYS_getsockname,
		libc::SYS_getsockopt,
		libc::SYS_gettid,
		libc::SYS_gettimeofday,
		libc::SYS_getuid,
		libc::SYS_getxattr,
		libc::SYS_inotify_add_watch,
		libc::SYS_inotify_init1,
		libc::SYS_inotify_rm_watch,
		libc::SYS_io_cancel,
		libc::SYS_io_destroy,
		libc::SYS_io_getevents,
		libc::SYS_ioprio_get,
		libc::SYS_ioprio_set,
		libc::SYS_io_setup,
		libc::SYS_io_submit,
		libc::SYS_ioctl,
		libc::SYS_kill,
		libc::SYS_landlock_add_rule,
		libc::SYS_landlock_create_ruleset,
		libc::SYS_landlock_restrict_self,
		libc::SYS_lgetxattr,
		libc::SYS_linkat,
		libc::SYS_listen,
		libc::SYS_listxattr,
		libc::SYS_llistxattr,
		libc::SYS_lremovexattr,
		libc::SYS_lseek,
		libc::SYS_lsetxattr,
		libc::SYS_madvise,
		libc::SYS_membarrier,
		libc::SYS_memfd_create,
		libc::SYS_mincore,
		libc::SYS_mkdirat,
		libc::SYS_mknodat,
		libc::SYS_mlock,
		libc::SYS_mlock2,
		libc::SYS_mlockall,
		libc::SYS_mmap,
		libc::SYS_mprotect,
		libc::SYS_mq_getsetattr,
		libc::SYS_mq_notify,
		libc::SYS_mq_open,
		libc::SYS_mq_timedreceive,
		libc::SYS_mq_timedsend,
		libc::SYS_mq_unlink,
		libc::SYS_mremap,
		libc::SYS_msgctl,
		libc::SYS_msgget,
		libc::SYS_msgrcv,
		libc::SYS_msgsnd,
		libc::SYS_msync,
		libc::SYS_munlock,
		libc::SYS_munlockall,
		libc::SYS_munmap,
		libc::SYS_nanosleep,
		libc::SYS_newfstatat,
		libc::SYS_openat,
		libc::SYS_openat2,
		libc::SYS_pidfd_open,
		libc::SYS_pidfd_send_signal,
		libc::SYS_pipe2,
		libc::SYS_ppoll,
		libc::SYS_prctl,
		libc::SYS_pread64,
		libc::SYS_preadv,
		libc::SYS_preadv2,
		libc::SYS_prlimit64,
		libc::SYS_pselect6,
		libc::SYS_pwrite64,
		libc::SYS_pwritev,
		libc::SYS_pwritev2,
		libc::SYS_read,
		libc::SYS_readahead,
		libc::SYS_readlinkat,
		libc::SYS_readv,
		libc::SYS_recvmmsg,
		libc::SYS_recvfrom,
		libc::SYS_recvmsg,
		libc::SYS_remap_file_pages,
		libc::SYS_removexattr,
		libc::SYS_renameat2,
		libc::SYS_restart_syscall,
		libc::SYS_rseq,
		libc::SYS_rt_sigaction,
		libc::SYS_rt_sigpending,
		libc::SYS_rt_sigprocmask,
		libc::SYS_rt_sigqueueinfo,
		libc::SYS_rt_sigreturn,
		libc::SYS_rt_sigsuspend,
		libc::SYS_rt_sigtimedwait,
		libc::SYS_rt_tgsigqueueinfo,
		libc::SYS_sched_getaffinity,
		libc::SYS_sched_getattr,
		libc::SYS_sched_getparam,
		libc::SYS_sched_get_priority_max,
		libc::SYS_sched_get_priority_min,
		libc::SYS_sched_getscheduler,
		libc::SYS_sched_rr_get_interval,
		libc::SYS_sched_setaffinity,
		libc::SYS_sched_setattr,
		libc::SYS_sched_setparam,
		libc::SYS_sched_setscheduler,
		libc::SYS_sched_yield,
		libc::SYS_seccomp,
		libc::SYS_semctl,
		libc::SYS_semget,
		libc::SYS_semop,
		libc::SYS_semtimedop,
		libc::SYS_sendfile,
		libc::SYS_sendmmsg,
		libc::SYS_sendmsg,
		libc::SYS_sendto,
		libc::SYS_setfsgid,
		libc::SYS_setfsuid,
		libc::SYS_setgid,
		libc::SYS_setgroups,
		libc::SYS_setitimer,
		libc::SYS_setpgid,
		libc::SYS_setpriority,
		libc::SYS_setregid,
		libc::SYS_setresgid,
		libc::SYS_setresuid,
		libc::SYS_setreuid,
		libc::SYS_setrlimit,
		libc::SYS_set_robust_list,
		libc::SYS_setsid,
		libc::SYS_setsockopt,
		libc::SYS_set_tid_address,
		libc::SYS_setuid,
		libc::SYS_setxattr,
		libc::SYS_shmat,
		libc::SYS_shmctl,
		libc::SYS_shmdt,
		libc::SYS_shmget,
		libc::SYS_shutdown,
		libc::SYS_sigaltstack,
		libc::SYS_signalfd4,
		libc::SYS_socketpair,
		libc::SYS_splice,
		libc::SYS_statfs,
		libc::SYS_statx,
		libc::SYS_symlinkat,
		libc::SYS_sync,
		libc::SYS_syncfs,
		libc::SYS_sysinfo,
		libc::SYS_tee,
		libc::SYS_tgkill,
		libc::SYS_timer_create,
		libc::SYS_timer_delete,
		libc::SYS_timer_getoverrun,
		libc::SYS_timer_gettime,
		libc::SYS_timer_settime,
		libc::SYS_timerfd_create,
		libc::SYS_timerfd_gettime,
		libc::SYS_timerfd_settime,
		libc::SYS_times,
		libc::SYS_tkill,
		libc::SYS_truncate,
		libc::SYS_umask,
		libc::SYS_uname,
		libc::SYS_unlinkat,
		libc::SYS_utimensat,
		libc::SYS_wait4,
		libc::SYS_waitid,
		libc::SYS_write,
		libc::SYS_writev,
	];
	let mut syscalls = syscalls.map(syscall_number).to_vec();
	#[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
	syscalls.extend(
		[
			libc::SYS_memfd_secret,
			libc::SYS_mseal,
			libc::SYS_process_mrelease,
		]
		.map(syscall_number),
	);
	#[cfg(target_arch = "x86_64")]
	syscalls.extend(
		[
			libc::SYS_access,
			libc::SYS_alarm,
			libc::SYS_arch_prctl,
			libc::SYS_chmod,
			libc::SYS_chown,
			libc::SYS_creat,
			libc::SYS_dup2,
			libc::SYS_epoll_create,
			libc::SYS_epoll_wait,
			libc::SYS_eventfd,
			libc::SYS_fork,
			libc::SYS_getdents,
			libc::SYS_inotify_init,
			libc::SYS_lchown,
			libc::SYS_link,
			libc::SYS_lstat,
			libc::SYS_mkdir,
			libc::SYS_open,
			libc::SYS_pause,
			libc::SYS_pipe,
			libc::SYS_poll,
			libc::SYS_readlink,
			libc::SYS_rename,
			libc::SYS_renameat,
			libc::SYS_rmdir,
			libc::SYS_select,
			libc::SYS_signalfd,
			libc::SYS_stat,
			libc::SYS_sync_file_range,
			libc::SYS_symlink,
			libc::SYS_time,
			libc::SYS_unlink,
			libc::SYS_utime,
			libc::SYS_utimes,
			libc::SYS_vfork,
		]
		.map(syscall_number),
	);
	syscalls
}

#[cfg(not(any(
	target_arch = "aarch64",
	target_arch = "riscv64",
	target_arch = "x86_64"
)))]
fn allowed_syscalls() -> Vec<u32> {
	Vec::new()
}

fn syscall_number(syscall: libc::c_long) -> u32 {
	syscall.try_into().unwrap()
}

fn audit_architecture() -> tg::Result<u32> {
	#[cfg(target_arch = "aarch64")]
	return Ok(0xc000_00b7);
	#[cfg(target_arch = "riscv64")]
	return Ok(0xc000_00f3);
	#[cfg(target_arch = "x86_64")]
	return Ok(0xc000_003e);
	#[allow(unreachable_code)]
	Err(tg::error!(
		architecture = %std::env::consts::ARCH,
		"seccomp is not supported on this architecture"
	))
}

fn jump(code: u32, k: u32, jt: u8, jf: u8) -> libc::sock_filter {
	libc::sock_filter {
		code: code.try_into().unwrap(),
		jf,
		jt,
		k,
	}
}

fn statement(code: u32, k: u32) -> libc::sock_filter {
	libc::sock_filter {
		code: code.try_into().unwrap(),
		jf: 0,
		jt: 0,
		k,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn install_default_policy() {
		if std::env::var_os("TANGRAM_SECCOMP_TEST_CHILD").is_some() {
			let result = unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) };
			assert_eq!(result, 0);
			install(SeccompPolicy::Default).unwrap();

			let result = unsafe { libc::syscall(libc::SYS_bpf, 0, 0, 0) };
			assert_eq!(result, -1);
			assert_eq!(
				std::io::Error::last_os_error().raw_os_error(),
				Some(libc::EPERM)
			);

			let result =
				unsafe { libc::syscall(libc::SYS_socket, libc::AF_ALG, libc::SOCK_SEQPACKET, 0) };
			assert_eq!(result, -1);
			assert_eq!(
				std::io::Error::last_os_error().raw_os_error(),
				Some(libc::EPERM)
			);

			let result =
				unsafe { libc::syscall(libc::SYS_io_uring_setup, 0, std::ptr::null::<u8>()) };
			assert_eq!(result, -1);
			assert_eq!(
				std::io::Error::last_os_error().raw_os_error(),
				Some(libc::EPERM)
			);

			let result = unsafe { libc::syscall(libc::SYS_clone3, std::ptr::null::<u8>(), 0) };
			assert_eq!(result, -1);
			assert_eq!(
				std::io::Error::last_os_error().raw_os_error(),
				Some(libc::ENOSYS)
			);

			// Exercise the legacy syscalls because libc may use prlimit64 instead.
			let mut limit = libc::rlimit {
				rlim_cur: 0,
				rlim_max: 0,
			};
			// SAFETY: The pointer refers to valid storage for the resource limit.
			let result =
				unsafe { libc::syscall(libc::SYS_getrlimit, libc::RLIMIT_STACK, &raw mut limit) };
			assert_eq!(result, 0);
			// SAFETY: The pointer refers to the resource limit initialized by getrlimit.
			let result =
				unsafe { libc::syscall(libc::SYS_setrlimit, libc::RLIMIT_STACK, &raw const limit) };
			assert_eq!(result, 0);

			assert!(unsafe { libc::getpid() } > 0);
			assert!(unsafe { libc::syscall(libc::SYS_personality, u32::MAX) } >= 0);
			assert_eq!(std::thread::spawn(|| 42).join().unwrap(), 42);
			assert!(
				std::process::Command::new("true")
					.status()
					.unwrap()
					.success()
			);
			return;
		}

		let executable = std::env::current_exe().unwrap();
		let status = std::process::Command::new(executable)
			.arg("--exact")
			.arg("container::seccomp::tests::install_default_policy")
			.arg("--nocapture")
			.env("TANGRAM_SECCOMP_TEST_CHILD", "1")
			.status()
			.unwrap();
		assert!(status.success());
	}
}
