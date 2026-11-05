use {
	std::{
		io::{Read as _, Write as _},
		net::Ipv4Addr,
		os::{
			fd::{AsRawFd as _, BorrowedFd, OwnedFd},
			unix::fs::MetadataExt as _,
		},
		path::{Path, PathBuf},
		process::{ExitCode, Stdio},
	},
	tangram_client::prelude::*,
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
};

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Arg {
	pub(crate) fds: [i32; 6],
	pub(crate) gid: u32,
	pub(crate) hostname: Option<String>,
	pub(crate) mounts: Vec<tg::sandbox::Mount>,
	pub(crate) network: Option<Network>,
	pub(crate) pid: Option<u32>,
	pub(crate) restore: Vec<(PathBuf, bool)>,
	pub(crate) uid: u32,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Network {
	pub(crate) gateway: Ipv4Addr,
	pub(crate) guest: Ipv4Addr,
	pub(crate) name: String,
}

/// Configure prepared namespaces in a single-threaded helper before starting a runtime.
pub fn run() -> tg::Result<ExitCode> {
	let mut contents = Vec::new();
	std::io::stdin()
		.read_to_end(&mut contents)
		.map_err(|error| tg::error!(!error, "failed to read the sandbox configuration"))?;
	let mut arg: Arg = serde_json::from_slice(&contents)
		.map_err(|error| tg::error!(!error, "failed to deserialize the sandbox configuration"))?;
	for fd in arg.fds {
		crate::linux::validate_descriptor(fd)?;
	}

	// Configure networking in its owning user namespace before entering a nested runtime's user namespace.
	if let Some(network) = &arg.network {
		enter_user(arg.fds[5])?;
		enter(arg.fds[1], libc::CLONE_NEWNET)?;

		let mut netlink = crate::network::netlink::Netlink::new()?;
		netlink.link_rename(&network.name, "eth0")?;
		crate::network::host::disable_ipv6("eth0")?;
		netlink.addr_add_v4("eth0", network.guest, 16)?;
		netlink.link_set_up("eth0")?;
		netlink.link_set_up("lo")?;
		netlink.route_add_default_v4(network.gateway)?;
	}
	if arg.hostname.is_some() || !arg.mounts.is_empty() {
		enter_user(arg.fds[3])?;
	}
	// Clone the sources in a private host mount namespace before entering the sandbox mount namespace.
	let sources = if arg.mounts.is_empty() {
		Vec::new()
	} else {
		crate::linux::unshare(libc::CLONE_NEWNS, "failed to prepare the mount sources")?;
		arg.mounts
			.iter()
			.map(|mount| crate::mount::clone_source(&mount.source))
			.collect::<tg::Result<Vec<_>>>()?
	};
	// Translate the workload identity when the runtime added a child user namespace.
	let (uid, gid) = if let Some(pid) = arg.pid {
		(
			map_identity(pid, "uid_map", arg.uid)?,
			map_identity(pid, "gid_map", arg.gid)?,
		)
	} else {
		(arg.uid, arg.gid)
	};
	crate::linux::setresgid(gid)?;
	crate::linux::setresuid(uid)?;

	if arg.hostname.is_some() {
		enter(arg.fds[4], libc::CLONE_NEWUTS)?;
	}
	if !arg.mounts.is_empty() {
		enter(arg.fds[0], libc::CLONE_NEWNS)?;
	}

	// Preserve mandatory descendants before custom mounts cover their parents.
	arg.restore
		.sort_by_key(|(target, _)| target.components().count());
	let restore = arg
		.restore
		.iter()
		.map(|(target, readonly)| {
			let source = crate::mount::clone_target(arg.fds[2], target)?;
			Ok((source, target, *readonly))
		})
		.collect::<tg::Result<Vec<_>>>()?;

	// Apply only the requested changes.
	if let Some(hostname) = &arg.hostname {
		crate::linux::set_hostname(hostname)?;
	}
	let mut mounts = arg.mounts.iter().zip(&sources).collect::<Vec<_>>();
	mounts.sort_by_key(|(mount, _)| mount.target.components().count());
	for (mount, source) in mounts {
		crate::mount::bind_descriptor(source, arg.fds[2], &mount.target, mount.readonly)?;
	}
	for (source, target, readonly) in restore {
		crate::mount::bind_descriptor(&source, arg.fds[2], target, readonly)?;
	}

	Ok(ExitCode::SUCCESS)
}

/// Hold namespace descriptors in a clean process until the network helper has opened them.
pub fn hold(network: i32, user: i32) -> tg::Result<ExitCode> {
	crate::linux::validate_descriptor(network)?;
	crate::linux::validate_descriptor(user)?;
	enter_user(user)?;
	crate::linux::restrict_capabilities(rustix::thread::CapabilitySet::empty())?;
	crate::util::set_dumpable(true)?;
	std::io::stdout()
		.write_all(&[0])
		.map_err(|error| tg::error!(!error, "failed to signal namespace readiness"))?;
	std::io::stdout()
		.flush()
		.map_err(|error| tg::error!(!error, "failed to flush namespace readiness"))?;
	let mut byte = [0];
	let count = std::io::stdin()
		.read(&mut byte)
		.map_err(|error| tg::error!(!error, "failed to wait for namespace release"))?;
	if count != 0 {
		return Err(tg::error!("expected the namespace hold stream to close"));
	}
	Ok(ExitCode::SUCCESS)
}

pub(crate) async fn command(path: &Path, arg: &Arg) -> tg::Result<()> {
	let contents = serde_json::to_vec(arg)
		.map_err(|error| tg::error!(!error, "failed to serialize the sandbox configuration"))?;
	let mut command = tokio::process::Command::new(path);
	command
		.args(["sandbox", "configure"])
		.kill_on_drop(true)
		.stdin(Stdio::piped())
		.stdout(Stdio::null())
		.stderr(Stdio::piped());
	let fds = arg.fds;
	// SAFETY: The descriptors remain owned by the sandbox, and the closure only invokes async-signal-safe fcntl calls.
	unsafe {
		command.pre_exec(move || {
			for fd in fds {
				if libc::fcntl(fd, libc::F_SETFD, 0) < 0 {
					return Err(std::io::Error::last_os_error());
				}
			}
			Ok(())
		});
	}
	let mut child = command
		.spawn()
		.map_err(|error| tg::error!(!error, "failed to start the sandbox configuration helper"))?;
	let mut stdin = child.stdin.take().unwrap();
	stdin
		.write_all(&contents)
		.await
		.map_err(|error| tg::error!(!error, "failed to write the sandbox configuration"))?;
	drop(stdin);
	let output = child.wait_with_output().await.map_err(|error| {
		tg::error!(
			!error,
			"failed to wait for the sandbox configuration helper"
		)
	})?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		return Err(
			tg::error!(%stderr, status = %output.status, "failed to configure the warm sandbox"),
		);
	}
	Ok(())
}

pub(crate) async fn hold_command(
	path: &Path,
	network: &OwnedFd,
	user: &OwnedFd,
) -> tg::Result<tokio::process::Child> {
	let mut command = tokio::process::Command::new(path);
	command
		.args([
			"sandbox",
			"configure",
			"--network-namespace-fd",
			&network.as_raw_fd().to_string(),
			"--user-namespace-fd",
			&user.as_raw_fd().to_string(),
		])
		.env_clear()
		.kill_on_drop(true)
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped());
	let fds = [network.as_raw_fd(), user.as_raw_fd()];
	// SAFETY: The namespace descriptors remain owned until the helper is ready, and the closure only calls fcntl.
	unsafe {
		command.pre_exec(move || {
			for fd in fds {
				if libc::fcntl(fd, libc::F_SETFD, 0) < 0 {
					return Err(std::io::Error::last_os_error());
				}
			}
			Ok(())
		});
	}
	let mut child = command
		.spawn()
		.map_err(|error| tg::error!(!error, "failed to start the namespace holder"))?;
	let mut stdout = child.stdout.take().unwrap();
	let mut byte = [0];
	if let Err(error) = stdout.read_exact(&mut byte).await {
		drop(child.stdin.take());
		let output = child
			.wait_with_output()
			.await
			.map_err(|error| tg::error!(!error, "failed to wait for the namespace holder"))?;
		let stderr = String::from_utf8_lossy(&output.stderr);
		return Err(tg::error!(!error, %stderr, "failed to prepare the namespace holder"));
	}
	Ok(child)
}

fn enter(fd: i32, kind: i32) -> tg::Result<()> {
	// SAFETY: The descriptor identifies the prepared namespace, and the helper has no other threads.
	if unsafe { libc::setns(fd, kind) } != 0 {
		return Err(tg::error!(
			kind = %kind,
			source = std::io::Error::last_os_error(),
			"failed to enter a prepared sandbox namespace"
		));
	}
	Ok(())
}

fn enter_user(fd: i32) -> tg::Result<()> {
	let current = std::fs::metadata("/proc/self/ns/user")
		.map_err(|error| tg::error!(!error, "failed to inspect the helper user namespace"))?;
	// SAFETY: The inherited descriptor is borrowed for the metadata check and stays open until the helper exits.
	let target = rustix::fs::fstat(unsafe { BorrowedFd::borrow_raw(fd) })
		.map_err(|error| tg::error!(!error, "failed to inspect the sandbox user namespace"))?;
	if current.ino() != target.st_ino {
		enter(fd, libc::CLONE_NEWUSER)?;
	}
	Ok(())
}

fn map_identity(pid: u32, name: &str, id: u32) -> tg::Result<u32> {
	let contents = std::fs::read_to_string(format!("/proc/{pid}/{name}"))
		.map_err(|error| tg::error!(!error, "failed to read the runtime identity map"))?;
	for line in contents.lines() {
		let fields = line
			.split_ascii_whitespace()
			.map(str::parse::<u32>)
			.collect::<Result<Vec<_>, _>>()
			.map_err(|error| tg::error!(!error, "failed to parse the runtime identity map"))?;
		let [guest, owner, count] = fields.as_slice() else {
			return Err(tg::error!("the runtime identity map is invalid"));
		};
		if let Some(offset) = id.checked_sub(*guest).filter(|offset| offset < count) {
			return owner
				.checked_add(offset)
				.ok_or_else(|| tg::error!("the mapped identity is too large"));
		}
	}
	Err(tg::error!("the runtime identity is absent from its map"))
}
