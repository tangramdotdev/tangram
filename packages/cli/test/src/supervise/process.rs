use {
	crate::Result,
	std::{
		io,
		os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
		time::Instant,
	},
};

#[cfg(target_os = "macos")]
const PROC_PGRP_ONLY: u32 = 2;

/// A process in the process table.
#[derive(Clone, Debug)]
pub struct Entry {
	pub command: String,
	#[cfg_attr(not(target_os = "macos"), expect(dead_code))]
	pub group: libc::pid_t,
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub parent: libc::pid_t,
	pub pid: libc::pid_t,
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub session: libc::pid_t,
}

/// The outcome of reaping exited children.
pub struct Reap {
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub children: bool,
	pub statuses: Vec<(libc::pid_t, libc::c_int)>,
}

pub fn errno() -> libc::c_int {
	// SAFETY: The errno location is valid for the current thread.
	unsafe { *errno_location() }
}

pub fn set_errno(value: libc::c_int) {
	// SAFETY: The errno location is valid for the current thread.
	unsafe {
		*errno_location() = value;
	}
}

pub fn pipe() -> Result<(OwnedFd, OwnedFd)> {
	let mut fds = [0; 2];
	// SAFETY: The array is valid for writes of two file descriptors.
	let result = unsafe { libc::pipe(fds.as_mut_ptr()) };
	if result != 0 {
		return Err(io::Error::last_os_error().into());
	}
	// SAFETY: The pipe call succeeded, so both file descriptors are open and owned by this process.
	let (read, write) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
	set_cloexec(&read, true)?;
	set_cloexec(&write, true)?;
	Ok((read, write))
}

pub fn set_cloexec(fd: &OwnedFd, cloexec: bool) -> Result<()> {
	set_fd_flag(
		fd.as_raw_fd(),
		libc::F_GETFD,
		libc::F_SETFD,
		libc::FD_CLOEXEC,
		cloexec,
	)
}

pub fn set_nonblocking(fd: &OwnedFd) -> Result<()> {
	set_fd_flag(
		fd.as_raw_fd(),
		libc::F_GETFL,
		libc::F_SETFL,
		libc::O_NONBLOCK,
		true,
	)
}

/// Poll the file descriptors until one is ready or the deadline passes. An interrupted poll returns early without an error.
pub fn poll(fds: &mut [libc::pollfd], deadline: Option<Instant>) -> Result<()> {
	let timeout = deadline.map_or(-1, |deadline| {
		let remaining = deadline.saturating_duration_since(Instant::now());
		let milliseconds = remaining.as_micros().div_ceil(1000);
		libc::c_int::try_from(milliseconds).unwrap_or(libc::c_int::MAX)
	});
	let count = libc::nfds_t::try_from(fds.len()).unwrap();
	// SAFETY: The slice is valid for reads and writes of its length.
	let result = unsafe { libc::poll(fds.as_mut_ptr(), count, timeout) };
	if result < 0 {
		let error = io::Error::last_os_error();
		if error.raw_os_error() == Some(libc::EINTR) {
			return Ok(());
		}
		return Err(error.into());
	}
	Ok(())
}

/// Send a signal to a process, ignoring a process that no longer exists.
pub fn kill(pid: libc::pid_t, signal: libc::c_int) {
	// SAFETY: kill has no memory safety preconditions.
	unsafe {
		libc::kill(pid, signal);
	}
}

/// Reap every exited child without blocking.
pub fn reap() -> Reap {
	let mut statuses = Vec::new();
	loop {
		let mut status = 0;
		// SAFETY: The status is valid for writes.
		let pid = unsafe { libc::waitpid(-1, &raw mut status, libc::WNOHANG) };
		if pid > 0 {
			statuses.push((pid, status));
			continue;
		}
		let children = !(pid < 0 && errno() == libc::ECHILD);
		return Reap { children, statuses };
	}
}

/// Convert a wait status to an exit code, using the shell convention for signals.
#[must_use]
pub fn code(status: libc::c_int) -> u8 {
	if libc::WIFEXITED(status) {
		u8::try_from(libc::WEXITSTATUS(status)).unwrap_or(1)
	} else if libc::WIFSIGNALED(status) {
		u8::try_from(128 + libc::WTERMSIG(status)).unwrap_or(1)
	} else {
		1
	}
}

/// Get the processes descended from a process.
#[cfg(target_os = "linux")]
#[must_use]
pub fn descendants(table: &[Entry], root: libc::pid_t) -> Vec<Entry> {
	let mut output = Vec::new();
	let mut parents = vec![root];
	while let Some(parent) = parents.pop() {
		for entry in table.iter().filter(|entry| entry.parent == parent) {
			parents.push(entry.pid);
			output.push(entry.clone());
		}
	}
	output
}

#[must_use]
pub fn describe(entries: &[Entry]) -> Vec<String> {
	entries
		.iter()
		.map(|entry| format!("{} ({})", entry.pid, entry.command))
		.collect()
}

/// Read the process table, excluding zombies.
#[cfg(target_os = "linux")]
pub fn table() -> Vec<Entry> {
	let Ok(read_dir) = std::fs::read_dir("/proc") else {
		return Vec::new();
	};
	let mut table = Vec::new();
	for entry in read_dir.flatten() {
		let Some(pid) = entry
			.file_name()
			.to_str()
			.and_then(|name| name.parse::<libc::pid_t>().ok())
		else {
			continue;
		};
		let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
			continue;
		};
		let Some((_, fields)) = stat.rsplit_once(')') else {
			continue;
		};
		let fields = fields.split_whitespace().collect::<Vec<_>>();
		if fields.len() < 4 || fields[0] == "Z" || fields[0] == "X" {
			continue;
		}
		let (Ok(parent), Ok(group), Ok(session)) =
			(fields[1].parse(), fields[2].parse(), fields[3].parse())
		else {
			continue;
		};
		let command = std::fs::read(entry.path().join("cmdline"))
			.map(|bytes| {
				String::from_utf8_lossy(&bytes)
					.trim_end_matches('\0')
					.replace('\0', " ")
			})
			.unwrap_or_default();
		let entry = Entry {
			command,
			group,
			parent,
			pid,
			session,
		};
		table.push(entry);
	}
	table
}

/// Read the process table with ps, excluding zombies.
#[cfg(target_os = "macos")]
pub fn table() -> Vec<Entry> {
	let Ok(output) = std::process::Command::new("ps")
		.args(["-axo", "pid=,ppid=,pgid=,stat=,command="])
		.stderr(std::process::Stdio::null())
		.output()
	else {
		return Vec::new();
	};
	let stdout = String::from_utf8_lossy(&output.stdout);
	let mut table = Vec::new();
	for line in stdout.lines() {
		let mut fields = line.split_whitespace();
		let (Some(pid), Some(parent), Some(group), Some(state)) =
			(fields.next(), fields.next(), fields.next(), fields.next())
		else {
			continue;
		};
		if state.starts_with('Z') {
			continue;
		}
		let (Ok(pid), Ok(parent), Ok(group)) = (pid.parse(), parent.parse(), group.parse()) else {
			continue;
		};
		let command = fields.collect::<Vec<_>>().join(" ");
		let entry = Entry {
			command,
			group,
			parent,
			pid,
			session: 0,
		};
		table.push(entry);
	}
	table
}

/// Get the processes in a session.
#[cfg(target_os = "linux")]
#[must_use]
pub fn session(session: libc::pid_t) -> Vec<libc::pid_t> {
	table()
		.into_iter()
		.filter(|entry| entry.session == session)
		.map(|entry| entry.pid)
		.collect()
}

/// Get the processes in a process group.
#[cfg(target_os = "macos")]
#[must_use]
pub fn group(group: libc::pid_t) -> Vec<libc::pid_t> {
	let mut pids = vec![0 as libc::pid_t; 4096];
	let size = libc::c_int::try_from(std::mem::size_of_val(pids.as_slice())).unwrap();
	let group = u32::try_from(group).unwrap_or(0);
	// SAFETY: The buffer is valid for writes of its size in bytes.
	let bytes =
		unsafe { libc::proc_listpids(PROC_PGRP_ONLY, group, pids.as_mut_ptr().cast(), size) };
	let count = usize::try_from(bytes).unwrap_or(0) / std::mem::size_of::<libc::pid_t>();
	pids.truncate(count);
	pids.retain(|pid| *pid > 0);
	pids
}

/// Wait for processes that are not children of this process to exit. Return the processes that are still running at the deadline.
#[cfg(target_os = "linux")]
pub fn wait(pids: &[libc::pid_t], deadline: Instant) -> Result<Vec<libc::pid_t>> {
	// Open a pidfd for each process. A process that no longer exists has already exited.
	let mut handles = Vec::new();
	for pid in pids {
		// SAFETY: pidfd_open has no memory safety preconditions.
		let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, *pid, 0) };
		let Ok(fd) = libc::c_int::try_from(fd) else {
			continue;
		};
		if fd < 0 {
			continue;
		}
		// SAFETY: The syscall succeeded, so the file descriptor is open and owned by this process.
		let fd = unsafe { OwnedFd::from_raw_fd(fd) };
		handles.push((*pid, fd));
	}

	// Poll the pidfds until every process exits or the deadline passes.
	loop {
		if handles.is_empty() || Instant::now() >= deadline {
			break;
		}
		let mut fds = handles
			.iter()
			.map(|(_, fd)| libc::pollfd {
				events: libc::POLLIN,
				fd: fd.as_raw_fd(),
				revents: 0,
			})
			.collect::<Vec<_>>();
		poll(&mut fds, Some(deadline))?;
		let mut index = 0;
		handles.retain(|_| {
			let running = fds[index].revents == 0;
			index += 1;
			running
		});
	}

	let remaining = handles.into_iter().map(|(pid, _)| pid).collect();
	Ok(remaining)
}

/// Wait for processes that are not children of this process to exit. Return the processes that are still running at the deadline.
#[cfg(target_os = "macos")]
pub fn wait(pids: &[libc::pid_t], deadline: Instant) -> Result<Vec<libc::pid_t>> {
	// Create the queue.
	// SAFETY: kqueue has no memory safety preconditions.
	let queue = unsafe { libc::kqueue() };
	if queue < 0 {
		return Err(io::Error::last_os_error().into());
	}
	// SAFETY: The kqueue call succeeded, so the file descriptor is open and owned by this process.
	let queue = unsafe { OwnedFd::from_raw_fd(queue) };

	// Register an exit event for each process. A process that no longer exists has already exited.
	let mut remaining = std::collections::BTreeSet::new();
	for pid in pids {
		let change = libc::kevent {
			data: 0,
			fflags: libc::NOTE_EXIT,
			filter: libc::EVFILT_PROC,
			flags: libc::EV_ADD | libc::EV_ONESHOT,
			ident: usize::try_from(*pid).unwrap_or(0),
			udata: std::ptr::null_mut(),
		};
		// SAFETY: The change is valid for reads, and no events are requested.
		let result = unsafe {
			libc::kevent(
				queue.as_raw_fd(),
				&raw const change,
				1,
				std::ptr::null_mut(),
				0,
				std::ptr::null(),
			)
		};
		if result == 0 {
			remaining.insert(*pid);
		}
	}

	// Wait for the exit events until every process exits or the deadline passes.
	while !remaining.is_empty() {
		let now = Instant::now();
		if now >= deadline {
			break;
		}
		let duration = deadline - now;
		let timeout = libc::timespec {
			tv_nsec: libc::c_long::from(duration.subsec_nanos()),
			tv_sec: libc::time_t::try_from(duration.as_secs()).unwrap_or(libc::time_t::MAX),
		};
		// SAFETY: An all-zero kevent is a valid value.
		let mut events: [libc::kevent; 16] = unsafe { std::mem::zeroed() };
		// SAFETY: The events are valid for writes of their length, and the timeout is valid for reads.
		let count = unsafe {
			libc::kevent(
				queue.as_raw_fd(),
				std::ptr::null(),
				0,
				events.as_mut_ptr(),
				16,
				&raw const timeout,
			)
		};
		if count < 0 {
			let error = io::Error::last_os_error();
			if error.raw_os_error() == Some(libc::EINTR) {
				continue;
			}
			return Err(error.into());
		}
		for event in &events[..usize::try_from(count).unwrap_or(0)] {
			remaining.remove(&libc::pid_t::try_from(event.ident).unwrap_or(0));
		}
	}

	let remaining = remaining.into_iter().collect();
	Ok(remaining)
}

#[cfg(target_os = "linux")]
fn errno_location() -> *mut libc::c_int {
	// SAFETY: __errno_location has no preconditions.
	unsafe { libc::__errno_location() }
}

#[cfg(target_os = "macos")]
fn errno_location() -> *mut libc::c_int {
	// SAFETY: __error has no preconditions.
	unsafe { libc::__error() }
}

fn set_fd_flag(
	fd: libc::c_int,
	get: libc::c_int,
	set: libc::c_int,
	flag: libc::c_int,
	value: bool,
) -> Result<()> {
	// SAFETY: fcntl with these commands has no memory safety preconditions.
	let flags = unsafe { libc::fcntl(fd, get) };
	if flags < 0 {
		return Err(io::Error::last_os_error().into());
	}
	let flags = if value { flags | flag } else { flags & !flag };
	// SAFETY: fcntl with these commands has no memory safety preconditions.
	let result = unsafe { libc::fcntl(fd, set, flags) };
	if result < 0 {
		return Err(io::Error::last_os_error().into());
	}
	Ok(())
}
