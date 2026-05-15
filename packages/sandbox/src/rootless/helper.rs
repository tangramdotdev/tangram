use {
	super::subuid::UserRanges,
	std::{
		io::{Read as _, Write as _},
		os::{
			fd::{AsRawFd as _, FromRawFd as _, IntoRawFd as _, RawFd},
			unix::net::UnixStream,
		},
		process::Command,
	},
	tangram_client::prelude::*,
};

const READY_BYTE: u8 = 0;
const GO_BYTE: u8 = 1;

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

	// For now, pause forever. The request loop will replace this in the next
	// milestone.
	loop {
		unsafe {
			libc::pause();
		}
	}
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
	// Leak the child end so it stays open across `execve`. The helper takes
	// ownership via `from_raw_fd(control_fd)`.
	let child_raw = child.into_raw_fd();
	Ok((parent, child_raw))
}
