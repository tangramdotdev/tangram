use {
	super::{
		protocol::{
			self, OP_DESTROY, OP_SHUTDOWN, OP_SPAWN_NETNS, OP_SPAWN_PASTA, OP_SPAWN_WRAPPER,
			OP_WAIT_WRAPPER, Request, Response, decode_spawn_payload,
		},
		subuid::UserRanges,
	},
	std::{
		collections::HashMap,
		ffi::CString,
		io::{Read as _, Write as _},
		os::{
			fd::{AsRawFd as _, FromRawFd as _, IntoRawFd as _, RawFd},
			unix::{ffi::OsStrExt as _, net::UnixStream},
		},
		path::PathBuf,
		process::Command,
	},
	tangram_client::prelude::*,
};

const READY_BYTE: u8 = 0;
const GO_BYTE: u8 = 1;
const NETNS_DIR_ENV: &str = "TANGRAM_HELPER_NETNS_DIR";

/// Entry point for the rootless helper subprocess.
///
/// The control socket is a `UnixStream` inherited from the parent (the server),
/// passed by raw fd number. The handshake is:
///
/// 1. Helper unshares `CLONE_NEWUSER` (creates a child user namespace with an
///    empty `uid_map`).
/// 2. Helper writes `READY_BYTE` to signal it is ready for the server to
///    populate the `uid_map`.
/// 3. Server invokes `/usr/bin/newuidmap` and `/usr/bin/newgidmap` against the
///    helper's pid to write the full subuid/subgid range into the helper's
///    namespace.
/// 4. Server writes `GO_BYTE`.
/// 5. Helper unshares `CLONE_NEWNS` (now its mount namespace is owned by the
///    new user ns and `CAP_SYS_ADMIN` is sufficient for mounts in it).
/// 6. Helper enters its request loop.
pub fn helper_main(control_fd: RawFd) -> tg::Result<std::process::ExitCode> {
	// Set the parent-death signal so we die with the server.
	if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) } < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set the parent-death signal"));
	}

	// Unshare into a new user namespace. The uid_map is empty at this point;
	// processes here appear as the overflow uid (65534) until the parent writes
	// the map. Must be single-threaded — this is why the helper is dispatched
	// from `main()` before the tokio runtime spins up.
	if unsafe { libc::unshare(libc::CLONE_NEWUSER) } < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to unshare the user namespace"));
	}

	let mut control = unsafe { UnixStream::from_raw_fd(control_fd) };

	// Tell the parent we are ready for the uid/gid map to be written.
	control
		.write_all(&[READY_BYTE])
		.map_err(|error| tg::error!(!error, "failed to signal ready to the parent"))?;

	// Wait for the parent to confirm the map has been written.
	let mut buf = [0u8; 1];
	control
		.read_exact(&mut buf)
		.map_err(|error| tg::error!(!error, "failed to wait for go from the parent"))?;
	if buf[0] != GO_BYTE {
		return Err(tg::error!(byte = %buf[0], "unexpected handshake byte"));
	}

	// Unshare the mount namespace. The new mount ns is owned by our new user
	// ns, which now has the subuid range mapped, so we hold CAP_SYS_ADMIN for
	// mount(2) within it.
	if unsafe { libc::unshare(libc::CLONE_NEWNS) } < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to unshare the mount namespace"));
	}

	// Determine the per-server runtime directory for bind-mounted netns files.
	let netns_dir = netns_directory();
	std::fs::create_dir_all(&netns_dir).map_err(
		|error| tg::error!(!error, path = %netns_dir.display(), "failed to create the netns directory"),
	)?;

	let mut state = HelperState {
		netns_dir,
		sandboxes: HashMap::new(),
	};

	// Enter the request loop.
	loop {
		let Some(request) = protocol::read_request(&mut control)? else {
			break;
		};
		let response = handle_request(&mut state, &request);
		protocol::write_response(&mut control, &response)?;
		if request.opcode == OP_SHUTDOWN {
			break;
		}
	}

	state.cleanup();
	Ok(std::process::ExitCode::SUCCESS)
}

struct HelperState {
	netns_dir: PathBuf,
	sandboxes: HashMap<String, SandboxEntry>,
}

struct SandboxEntry {
	netns_holder_pid: libc::pid_t,
	netns_path: PathBuf,
	pasta_pid: Option<libc::pid_t>,
	wrapper_pid: Option<libc::pid_t>,
}

impl HelperState {
	fn cleanup(&mut self) {
		for (_, entry) in self.sandboxes.drain() {
			cleanup_entry(&entry);
		}
	}
}

fn cleanup_entry(entry: &SandboxEntry) {
	for pid in [
		entry.wrapper_pid,
		entry.pasta_pid,
		Some(entry.netns_holder_pid),
	]
	.into_iter()
	.flatten()
	{
		unsafe {
			libc::kill(pid, libc::SIGTERM);
			let mut status = 0;
			libc::waitpid(pid, &raw mut status, 0);
		}
	}
	if let Ok(path_c) = CString::new(entry.netns_path.as_os_str().as_bytes()) {
		unsafe {
			libc::umount2(path_c.as_ptr(), libc::MNT_DETACH);
		}
	}
	std::fs::remove_file(&entry.netns_path).ok();
}

fn handle_request(state: &mut HelperState, request: &Request) -> Response {
	let result = match request.opcode {
		OP_SPAWN_NETNS => handle_spawn_netns(state, &request.payload),
		OP_SPAWN_PASTA => handle_spawn_pasta(state, &request.payload),
		OP_SPAWN_WRAPPER => handle_spawn_wrapper(state, &request.payload),
		OP_WAIT_WRAPPER => handle_wait_wrapper(state, &request.payload),
		OP_DESTROY => handle_destroy(state, &request.payload),
		OP_SHUTDOWN => Ok(Vec::new()),
		other => Err(tg::error!(opcode = %other, "unknown request opcode")),
	};
	match result {
		Ok(payload) => Response::ok(payload),
		Err(error) => Response::err(format!("{error}")),
	}
}

fn handle_spawn_netns(state: &mut HelperState, payload: &[u8]) -> tg::Result<Vec<u8>> {
	let sandbox_id = std::str::from_utf8(payload)
		.map_err(|source| tg::error!(!source, "invalid sandbox id encoding"))?
		.to_owned();
	if state.sandboxes.contains_key(&sandbox_id) {
		return Err(tg::error!(id = %sandbox_id, "sandbox netns already exists"));
	}
	let netns_path = state.netns_dir.join(format!("sbx-{sandbox_id}"));

	// Create the placeholder file before forking so we can bind-mount onto it.
	std::fs::File::create(&netns_path).map(drop).map_err(
		|error| tg::error!(!error, path = %netns_path.display(), "failed to create the netns target file"),
	)?;

	// CLOEXEC sync pipe — child closes its write end on exec (sleep call) or
	// on death. The parent reads EOF and knows the child is set up. Here we
	// reuse the same pattern but with a manual `write` after bind-mount because
	// the netns holder does not exec — it pauses.
	let mut sync = [-1 as libc::c_int; 2];
	if unsafe { libc::pipe2(sync.as_mut_ptr(), libc::O_CLOEXEC) } < 0 {
		let error = std::io::Error::last_os_error();
		std::fs::remove_file(&netns_path).ok();
		return Err(tg::error!(!error, "failed to create the netns sync pipe"));
	}
	let sync_read = sync[0];
	let sync_write = sync[1];

	let pid = unsafe { libc::fork() };
	if pid < 0 {
		let error = std::io::Error::last_os_error();
		unsafe {
			libc::close(sync_read);
			libc::close(sync_write);
		}
		std::fs::remove_file(&netns_path).ok();
		return Err(tg::error!(!error, "failed to fork the netns holder"));
	}
	if pid == 0 {
		unsafe {
			libc::close(sync_read);
		}
		netns_holder_child(sync_write, &netns_path);
	}

	unsafe {
		libc::close(sync_write);
	}
	let mut buf = [0u8; 1];
	let read_result = unsafe { libc::read(sync_read, buf.as_mut_ptr().cast(), 1) };
	unsafe {
		libc::close(sync_read);
	}
	if read_result <= 0 {
		// Child died before bind-mounting; reap it and surface the error.
		let mut status = 0;
		unsafe {
			libc::waitpid(pid, &raw mut status, 0);
		}
		std::fs::remove_file(&netns_path).ok();
		return Err(tg::error!("the netns holder exited before bind-mounting"));
	}

	let entry = SandboxEntry {
		netns_holder_pid: pid,
		netns_path: netns_path.clone(),
		pasta_pid: None,
		wrapper_pid: None,
	};
	state.sandboxes.insert(sandbox_id, entry);

	Ok(netns_path.as_os_str().as_bytes().to_vec())
}

fn handle_spawn_pasta(state: &mut HelperState, payload: &[u8]) -> tg::Result<Vec<u8>> {
	let (sandbox_id, argv) = decode_spawn_payload(payload)?;
	let entry = state
		.sandboxes
		.get_mut(&sandbox_id)
		.ok_or_else(|| tg::error!(id = %sandbox_id, "unknown sandbox"))?;
	if entry.pasta_pid.is_some() {
		return Err(tg::error!(id = %sandbox_id, "pasta already spawned"));
	}
	if argv.is_empty() {
		return Err(tg::error!("pasta argv is empty"));
	}
	let pid = fork_exec(None, &argv)?;
	entry.pasta_pid = Some(pid);
	Ok(pid.to_le_bytes().to_vec())
}

fn handle_spawn_wrapper(state: &mut HelperState, payload: &[u8]) -> tg::Result<Vec<u8>> {
	let (sandbox_id, argv) = decode_spawn_payload(payload)?;
	let entry = state
		.sandboxes
		.get_mut(&sandbox_id)
		.ok_or_else(|| tg::error!(id = %sandbox_id, "unknown sandbox"))?;
	if entry.wrapper_pid.is_some() {
		return Err(tg::error!(id = %sandbox_id, "wrapper already spawned"));
	}
	if argv.is_empty() {
		return Err(tg::error!("wrapper argv is empty"));
	}
	let netns_path = entry.netns_path.clone();
	let pid = fork_exec(Some(&netns_path), &argv)?;
	entry.wrapper_pid = Some(pid);
	Ok(pid.to_le_bytes().to_vec())
}

fn handle_wait_wrapper(state: &mut HelperState, payload: &[u8]) -> tg::Result<Vec<u8>> {
	let sandbox_id = std::str::from_utf8(payload)
		.map_err(|source| tg::error!(!source, "invalid sandbox id encoding"))?;
	let wrapper_pid = state
		.sandboxes
		.get(sandbox_id)
		.and_then(|entry| entry.wrapper_pid)
		.ok_or_else(|| tg::error!(id = %sandbox_id, "no wrapper to wait on"))?;
	let mut status = 0;
	let result = unsafe { libc::waitpid(wrapper_pid, &raw mut status, 0) };
	if result < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to waitpid the wrapper"));
	}
	if let Some(entry) = state.sandboxes.get_mut(sandbox_id) {
		entry.wrapper_pid = None;
	}
	let exit_code = if libc::WIFEXITED(status) {
		libc::WEXITSTATUS(status)
	} else if libc::WIFSIGNALED(status) {
		128 + libc::WTERMSIG(status)
	} else {
		-1
	};
	Ok(i32::to_le_bytes(exit_code).to_vec())
}

/// Fork a child. If `netns_path` is `Some`, the child enters that netns via
/// `setns` before exec. Then the child `execvp`s `argv[0]` with `argv` as its
/// argument vector. Returns the new child's pid in the parent.
fn fork_exec(netns_path: Option<&std::path::Path>, argv: &[String]) -> tg::Result<libc::pid_t> {
	let argv_c: Vec<CString> = argv
		.iter()
		.map(|s| {
			CString::new(s.as_bytes())
				.map_err(|source| tg::error!(!source, arg = %s, "argv entry contains a NUL"))
		})
		.collect::<tg::Result<Vec<_>>>()?;
	let argv_ptrs: Vec<*const libc::c_char> = argv_c
		.iter()
		.map(|s| s.as_ptr())
		.chain(std::iter::once(std::ptr::null()))
		.collect();
	let netns_c = netns_path
		.map(|path| CString::new(path.as_os_str().as_bytes()))
		.transpose()
		.map_err(|source| tg::error!(!source, "netns path contains a NUL"))?;

	let pid = unsafe { libc::fork() };
	if pid < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to fork"));
	}
	if pid == 0 {
		// Enter the netns if requested.
		if let Some(netns_c) = netns_c.as_ref() {
			let fd = unsafe { libc::open(netns_c.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
			if fd < 0 {
				unsafe { libc::_exit(110) };
			}
			if unsafe { libc::setns(fd, libc::CLONE_NEWNET) } < 0 {
				unsafe { libc::_exit(111) };
			}
			unsafe { libc::close(fd) };
		}
		unsafe {
			libc::execvp(argv_ptrs[0], argv_ptrs.as_ptr());
		}
		unsafe { libc::_exit(127) };
	}
	Ok(pid)
}

fn handle_destroy(state: &mut HelperState, payload: &[u8]) -> tg::Result<Vec<u8>> {
	let sandbox_id = std::str::from_utf8(payload)
		.map_err(|source| tg::error!(!source, "invalid sandbox id encoding"))?;
	if let Some(entry) = state.sandboxes.remove(sandbox_id) {
		cleanup_entry(&entry);
	}
	Ok(Vec::new())
}

/// Body of the netns-holder child. Never returns.
fn netns_holder_child(sync_write: libc::c_int, netns_path: &std::path::Path) -> ! {
	let _ = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) };

	if unsafe { libc::unshare(libc::CLONE_NEWNET) } < 0 {
		unsafe { libc::_exit(101) };
	}

	let Ok(target_c) = CString::new(netns_path.as_os_str().as_bytes()) else {
		unsafe { libc::_exit(102) };
	};
	let source_c = c"/proc/self/ns/net";
	let result = unsafe {
		libc::mount(
			source_c.as_ptr(),
			target_c.as_ptr(),
			std::ptr::null(),
			libc::MS_BIND,
			std::ptr::null(),
		)
	};
	if result < 0 {
		unsafe { libc::_exit(103) };
	}

	// Signal the helper that the bind-mount is in place.
	let ok = 1u8;
	let written = unsafe { libc::write(sync_write, std::ptr::addr_of!(ok).cast(), 1) };
	if written != 1 {
		unsafe { libc::_exit(104) };
	}
	unsafe {
		libc::close(sync_write);
	}

	loop {
		unsafe {
			libc::pause();
		}
	}
}

fn netns_directory() -> PathBuf {
	if let Ok(dir) = std::env::var(NETNS_DIR_ENV)
		&& !dir.is_empty()
	{
		return PathBuf::from(dir);
	}
	let xdg = std::env::var("XDG_RUNTIME_DIR")
		.ok()
		.filter(|s| !s.is_empty());
	let base = if let Some(xdg) = xdg {
		PathBuf::from(xdg)
	} else {
		let uid = unsafe { libc::geteuid() };
		PathBuf::from(format!("/run/user/{uid}"))
	};
	let parent_pid = unsafe { libc::getppid() };
	base.join("tangram")
		.join(parent_pid.to_string())
		.join("netns")
}

/// Server-side driver: given an open `UnixStream` to a freshly-spawned helper
/// subprocess and its pid, complete the uid/gid map handshake. Returns once
/// the helper has finished its initialization.
pub fn populate_helper_namespace(
	control: &mut UnixStream,
	helper_pid: libc::pid_t,
	ranges: &UserRanges,
) -> tg::Result<()> {
	// Wait for the helper to signal it has unshared its user namespace.
	let mut buf = [0u8; 1];
	control
		.read_exact(&mut buf)
		.map_err(|error| tg::error!(!error, "failed to wait for helper ready"))?;
	if buf[0] != READY_BYTE {
		return Err(tg::error!(byte = %buf[0], "unexpected handshake byte from helper"));
	}

	let host_uid = unsafe { libc::geteuid() };
	let host_gid = unsafe { libc::getegid() };
	let pid_string = helper_pid.to_string();

	// uid map: in-ns uid 0 -> host uid, then subuid range starting at in-ns uid 1.
	let uidmap_status = Command::new("/usr/bin/newuidmap")
		.arg(&pid_string)
		.arg("0")
		.arg(host_uid.to_string())
		.arg("1")
		.arg("1")
		.arg(ranges.uid.start.to_string())
		.arg(ranges.uid.count.to_string())
		.status()
		.map_err(|error| tg::error!(!error, "failed to run newuidmap"))?;
	if !uidmap_status.success() {
		return Err(tg::error!(
			status = %uidmap_status.code().unwrap_or(-1),
			"newuidmap exited with a non-zero status"
		));
	}

	// gid map: in-ns gid 0 -> host gid, then subgid range starting at in-ns gid 1.
	let gidmap_status = Command::new("/usr/bin/newgidmap")
		.arg(&pid_string)
		.arg("0")
		.arg(host_gid.to_string())
		.arg("1")
		.arg("1")
		.arg(ranges.gid.start.to_string())
		.arg(ranges.gid.count.to_string())
		.status()
		.map_err(|error| tg::error!(!error, "failed to run newgidmap"))?;
	if !gidmap_status.success() {
		return Err(tg::error!(
			status = %gidmap_status.code().unwrap_or(-1),
			"newgidmap exited with a non-zero status"
		));
	}

	// Signal the helper to proceed.
	control
		.write_all(&[GO_BYTE])
		.map_err(|error| tg::error!(!error, "failed to signal go to the helper"))?;
	Ok(())
}

/// Convenience: build a `Command` that invokes `tangram sandbox helper` with
/// the given control fd inherited. Caller is responsible for spawning it and
/// pairing the parent end of the socket with `populate_helper_namespace`.
#[must_use]
pub fn build_helper_command(tangram_path: &std::path::Path, control_fd: RawFd) -> Command {
	let mut command = Command::new(tangram_path);
	command
		.arg("sandbox")
		.arg("helper")
		.arg("--control-fd")
		.arg(control_fd.to_string());
	command
}

/// Create a socketpair suitable for the helper handshake. Returns
/// `(parent_end, child_end_raw_fd)`. The child end has `FD_CLOEXEC` cleared so
/// it survives `execve`.
pub fn socketpair() -> tg::Result<(UnixStream, RawFd)> {
	let (parent, child) =
		UnixStream::pair().map_err(|error| tg::error!(!error, "failed to create a socketpair"))?;
	let child_fd = child.as_raw_fd();
	let flags = unsafe { libc::fcntl(child_fd, libc::F_GETFD) };
	if flags < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to read the child fd flags"));
	}
	if unsafe { libc::fcntl(child_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			"failed to clear FD_CLOEXEC on the child end"
		));
	}
	let child_raw = child.into_raw_fd();
	Ok((parent, child_raw))
}
