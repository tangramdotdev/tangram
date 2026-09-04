use {
	crate::Sandbox,
	indoc::indoc,
	std::{
		ffi::{CStr, CString, OsStr},
		fmt::Write as _,
		mem::MaybeUninit,
		net::Ipv4Addr,
		os::{fd::AsRawFd as _, unix::ffi::OsStrExt as _},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

const DEFAULT_USER_BUFFER_SIZE: usize = 16 * 1024;

#[derive(Debug, Eq, PartialEq)]
struct User {
	gid: libc::gid_t,
	home: PathBuf,
	name: String,
	uid: libc::uid_t,
}

enum UserQuery {
	Name(CString),
	Uid(libc::uid_t),
}

pub(crate) async fn spawn(
	arg: &crate::Arg,
	serve_arg: &crate::serve::Arg,
	network: Option<&mut crate::network::Network>,
) -> tg::Result<tokio::process::Child> {
	let crate::Isolation::Container(isolation) = &arg.isolation else {
		unreachable!()
	};
	let network_arg = match network.as_deref() {
		None => None,
		Some(crate::network::Network::Host) => Some("host".to_owned()),
		Some(crate::network::Network::Passt(_)) => {
			return Err(tg::error!(
				"container sandboxes do not support passt networking"
			));
		},
		Some(crate::network::Network::Pasta(_)) => Some("pasta".to_owned()),
		Some(crate::network::Network::Tap(_)) => {
			return Err(tg::error!(
				"container sandboxes do not support tap networking"
			));
		},
		Some(crate::network::Network::Veth(_)) => Some("veth".to_owned()),
	};
	let mut veth = match network.as_deref() {
		Some(crate::network::Network::Veth(network)) => {
			let index = arg.index;
			let bridge_name = network.bridge_name().to_owned();
			Some(
				tokio::task::spawn_blocking(move || {
					crate::network::veth::Pair::new(index, &bridge_name)
				})
				.await
				.map_err(|error| tg::error!(!error, "the veth creation task panicked"))??,
			)
		},
		_ => None,
	};
	prepare_sandbox_directory(&arg.path)?;
	let user = prepare_etc_files(&arg.path, network.as_deref(), &arg.dns)?;
	let upper_path = Sandbox::host_upper_path_from_root(&arg.path);
	for mount in &arg.mounts {
		crate::container::root::ensure_mount_target(&arg.rootfs_path, &upper_path, mount)?;
	}
	let stdio = matches!(serve_arg.url.scheme(), Some("http+stdio"));
	let init_arg = super::init::Arg {
		serve: serve_arg.clone(),
	};
	let mut command = tokio::process::Command::new(&arg.tangram_path);
	command.arg("sandbox").arg("container").arg("run");
	command
		.arg("--index")
		.arg(arg.index.to_string())
		.arg("--unshare-all")
		.arg("--as-pid-1")
		.arg("--die-with-parent")
		.arg("--new-session")
		.arg("--uid")
		.arg(user.uid.to_string())
		.arg("--gid")
		.arg(user.gid.to_string())
		.arg("--nice")
		.arg(arg.nice.to_string())
		.arg("--chdir")
		.arg("/")
		.arg("--clearenv")
		.arg("--overlay-src")
		.arg(&arg.rootfs_path)
		.arg("--overlay")
		.arg(Sandbox::host_upper_path_from_root(&arg.path))
		.arg(Sandbox::host_work_path_from_root(&arg.path))
		.arg("/")
		.arg("--dev")
		.arg("/dev")
		.arg("--proc")
		.arg("/proc")
		.arg("--bind")
		.arg(Sandbox::host_tmp_path_from_root(&arg.path))
		.arg(Sandbox::guest_tmp_path_from_root(&arg.path));
	if let Some(network_arg) = &network_arg {
		command.arg("--network").arg(network_arg);
	}
	if let Some(crate::network::Network::Pasta(network)) = network.as_deref()
		&& let Some(guest_pipe) = network.guest_pipe()
	{
		command
			.arg("--network-fd")
			.arg(guest_pipe.as_raw_fd().to_string());
	}
	if let Some(veth) = veth.as_ref() {
		command
			.arg("--network-fd")
			.arg(veth.guest_pipe.as_ref().unwrap().as_raw_fd().to_string());
	}
	if let Some(crate::network::Network::Veth(network)) = network.as_deref() {
		command
			.arg("--gateway-ip")
			.arg(network.gateway_ip().to_string());
	}
	if let Some(crate::network::Network::Veth(network)) = network.as_deref() {
		command
			.arg("--guest-ip")
			.arg(network.guest_ip().to_string());
	}
	if let Some(fuse_fd) = &arg.fuse_fd {
		command
			.arg("--fuse-fd")
			.arg(fuse_fd.as_raw_fd().to_string())
			.arg("--fuse-path")
			.arg(&arg.store_path);
		// Clear CLOEXEC on the FUSE socket after forking so only the sandbox inherits it.
		let raw = fuse_fd.as_raw_fd();
		// SAFETY: The pre_exec closure only calls async-signal-safe operations.
		unsafe {
			command.pre_exec(move || {
				let flags = libc::fcntl(raw, libc::F_GETFD);
				if flags < 0 {
					return Err(std::io::Error::last_os_error());
				}
				if libc::fcntl(raw, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
					return Err(std::io::Error::last_os_error());
				}
				Ok(())
			});
		}
	}
	if let Some(hostname) = &arg.hostname {
		command.arg("--hostname").arg(hostname);
	}
	if let Some(cpu) = arg.cpu {
		command.arg("--cgroup-cpu").arg(cpu.to_string());
	}
	if let Some(memory) = arg.memory {
		command.arg("--cgroup-memory").arg(memory.to_string());
	}
	command
		.arg("--setenv")
		.arg("HOME")
		.arg(&user.home)
		.arg("--setenv")
		.arg("SSL_CERT_DIR")
		.arg(Sandbox::guest_ssl_cert_dir())
		.arg("--ro-bind")
		.arg(Sandbox::host_passwd_path_from_root(&arg.path))
		.arg("/etc/passwd")
		.arg("--ro-bind")
		.arg(Sandbox::host_nsswitch_path_from_root(&arg.path))
		.arg("/etc/nsswitch.conf")
		.arg("--ro-bind")
		.arg(&arg.store_path)
		.arg(Sandbox::guest_store_path_from_host_store_path(
			&arg.store_path,
		))
		.arg("--ro-bind")
		.arg(&arg.tangram_path)
		.arg(Sandbox::guest_libexec_tangram_path())
		.arg("--bind")
		.arg(
			arg.tangram_socket_path
				.as_ref()
				.ok_or_else(|| tg::error!("missing the tangram socket path"))?,
		)
		.arg(Sandbox::guest_tangram_socket_path_from_root(&arg.path))
		.arg("--bind")
		.arg(Sandbox::host_output_path_from_root(&arg.path))
		.arg(Sandbox::guest_output_path_from_root(&arg.path));
	if !stdio {
		command
			.arg("--bind")
			.arg(Sandbox::host_listen_path_from_root(&arg.path))
			.arg(Sandbox::guest_listen_path_from_root(&arg.path));
	}
	let resolv_conf_path = Sandbox::host_resolv_conf_path_from_root(&arg.path);
	if resolv_conf_path.exists() {
		command
			.arg("--ro-bind")
			.arg(&resolv_conf_path)
			.arg("/etc/resolv.conf");
	}
	for mount in &arg.mounts {
		command
			.arg(if mount.readonly {
				"--ro-bind"
			} else {
				"--bind"
			})
			.arg(&mount.source)
			.arg(&mount.target);
	}
	let cgroup_name = arg
		.path
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("sandbox");
	command
		.arg("--cgroup")
		.arg(cgroup_name)
		.arg("--cgroup-memory-oom-group");
	if let Some(max_pids) = isolation.max_pids {
		command.arg("--cgroup-pids").arg(max_pids.to_string());
	}
	command
		.arg("--")
		.arg(Sandbox::guest_tangram_path_from_host_tangram_path(
			&arg.tangram_path,
		))
		.arg("sandbox")
		.arg("container")
		.arg("init")
		.arg("--output-path")
		.arg(&init_arg.serve.output_path)
		.arg("--url")
		.arg(init_arg.serve.url.to_string())
		.arg("--tangram-path")
		.arg(&init_arg.serve.tangram_path);
	for path in &init_arg.serve.library_paths {
		command.arg("--library-path").arg(path);
	}
	command
		.kill_on_drop(true)
		.stdin(std::process::Stdio::piped())
		.stdout(std::process::Stdio::piped())
		.stderr(std::process::Stdio::inherit());
	let child = command
		.spawn()
		.map_err(|error| tg::error!(!error, "failed to spawn sandbox container"))?;
	if let Some(crate::network::Network::Pasta(network)) = network {
		network.take_guest_pipe();
		let pid = child
			.id()
			.ok_or_else(|| tg::error!("no child pid available"))?;
		let pid = i32::try_from(pid).map_err(|error| tg::error!(!error, "invalid child pid"))?;
		network.start_netns(pid).await?;
	}
	if let Some(veth) = veth.as_mut() {
		drop(veth.guest_pipe.take());
		let pid = child
			.id()
			.ok_or_else(|| tg::error!("no child pid available"))?;
		let pid = i32::try_from(pid).map_err(|error| tg::error!(!error, "invalid child pid"))?;
		veth.connect(pid).await?;
	}
	Ok(child)
}

fn prepare_sandbox_directory(sandbox_path: &Path) -> tg::Result<()> {
	for path in [
		Sandbox::host_output_path_from_root(sandbox_path),
		Sandbox::host_scratch_path_from_root(sandbox_path),
		Sandbox::host_tmp_path_from_root(sandbox_path),
		Sandbox::host_etc_path_from_root(sandbox_path),
		Sandbox::host_upper_path_from_root(sandbox_path),
		Sandbox::host_work_path_from_root(sandbox_path),
	] {
		std::fs::create_dir_all(&path).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the sandbox path"),
		)?;
	}
	let permissions =
		<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o1777);
	let tmp_path = Sandbox::host_tmp_path_from_root(sandbox_path);
	std::fs::set_permissions(&tmp_path, permissions).map_err(|error| {
		tg::error!(
			!error,
			path = %tmp_path.display(),
			"failed to set sandbox path permissions"
		)
	})?;
	let upper_path = Sandbox::host_upper_path_from_root(sandbox_path);
	let tangram_path = upper_path.join("opt/tangram");
	std::fs::create_dir_all(&tangram_path).map_err(|error| {
		tg::error!(
			!error,
			path = %tangram_path.display(),
			"failed to create the sandbox path"
		)
	})?;
	Ok(())
}

fn prepare_etc_files(
	sandbox_path: &Path,
	network: Option<&crate::network::Network>,
	dns: &[Ipv4Addr],
) -> tg::Result<User> {
	let user = resolve_user(None)?;
	let passwd = render_passwd(&user);
	std::fs::write(Sandbox::host_passwd_path_from_root(sandbox_path), passwd)
		.map_err(|error| tg::error!(!error, "failed to write /etc/passwd"))?;
	let nsswitch = indoc!(
		"
			passwd: files compat
			shadow: files compat
			hosts: files dns compat
		"
	);
	std::fs::write(
		Sandbox::host_nsswitch_path_from_root(sandbox_path),
		nsswitch,
	)
	.map_err(|error| tg::error!(!error, "failed to write /etc/nsswitch.conf"))?;
	match network {
		Some(crate::network::Network::Host) if Path::new("/etc/resolv.conf").exists() => {
			let path = Sandbox::host_resolv_conf_path_from_root(sandbox_path);
			std::fs::copy("/etc/resolv.conf", path)
				.map_err(|error| tg::error!(!error, "failed to stage /etc/resolv.conf"))?;
		},
		Some(crate::network::Network::Pasta(_)) => {
			let contents = format!("nameserver {}\n", crate::network::pasta::DNS_FORWARD_IP);
			let path = Sandbox::host_resolv_conf_path_from_root(sandbox_path);
			std::fs::write(path, contents)
				.map_err(|error| tg::error!(!error, "failed to stage /etc/resolv.conf"))?;
		},
		Some(crate::network::Network::Veth(_)) => {
			let mut contents = String::new();
			for server in dns {
				use std::fmt::Write as _;
				writeln!(&mut contents, "nameserver {server}").unwrap();
			}
			let path = Sandbox::host_resolv_conf_path_from_root(sandbox_path);
			std::fs::write(path, contents)
				.map_err(|error| tg::error!(!error, "failed to stage /etc/resolv.conf"))?;
		},
		_ => (),
	}
	Ok(user)
}

fn render_passwd(user: &User) -> String {
	let mut passwd = String::from(
		"root:!:0:0:root:/root:/bin/false\nnobody:!:65534:65534:nobody:/nonexistent:/bin/false\n",
	);
	if user.uid != 0 && user.uid != 65534 {
		writeln!(
			passwd,
			"{}:!:{}:{}:{}:{}:/bin/false",
			user.name,
			user.uid,
			user.gid,
			user.name,
			user.home.display(),
		)
		.unwrap();
	}
	passwd
}

fn resolve_user(name: Option<&str>) -> tg::Result<User> {
	let query = if let Some(name) = name {
		let name = CString::new(OsStr::new(name).as_bytes())
			.map_err(|error| tg::error!(!error, "failed to encode the user name"))?;
		UserQuery::Name(name)
	} else {
		// SAFETY: This function has no preconditions.
		let uid = unsafe { libc::getuid() };
		UserQuery::Uid(uid)
	};

	resolve_user_with_checkpoint(&query, || {})
}

fn resolve_user_with_checkpoint(query: &UserQuery, checkpoint: impl FnOnce()) -> tg::Result<User> {
	// Determine the initial buffer size.
	// SAFETY: This function has no preconditions.
	let configured_buffer_size = unsafe { libc::sysconf(libc::_SC_GETPW_R_SIZE_MAX) };
	let mut buffer_size = usize::try_from(configured_buffer_size)
		.ok()
		.filter(|size| *size > 0)
		.unwrap_or(DEFAULT_USER_BUFFER_SIZE);
	loop {
		let mut buffer = vec![0u8; buffer_size];
		let mut passwd = MaybeUninit::<libc::passwd>::uninit();
		let mut result = std::ptr::null_mut();

		// Resolve the record.
		// SAFETY: The name pointer, output pointers, and buffer are valid for the call.
		let status = unsafe {
			match query {
				UserQuery::Name(name) => libc::getpwnam_r(
					name.as_ptr(),
					passwd.as_mut_ptr(),
					buffer.as_mut_ptr().cast(),
					buffer.len(),
					&raw mut result,
				),
				UserQuery::Uid(uid) => libc::getpwuid_r(
					*uid,
					passwd.as_mut_ptr(),
					buffer.as_mut_ptr().cast(),
					buffer.len(),
					&raw mut result,
				),
			}
		};
		if status == libc::ERANGE {
			buffer_size = buffer_size
				.checked_mul(2)
				.ok_or_else(|| tg::error!("the user record is too large"))?;
			continue;
		}
		if status != 0 {
			let error = std::io::Error::from_raw_os_error(status);
			return Err(tg::error!(!error, "failed to resolve the user"));
		}
		if result.is_null() {
			return Err(tg::error!("failed to resolve the user"));
		}

		// Copy the record while its buffer is live.
		// SAFETY: A successful lookup with a non-null result initialized the passwd record.
		let passwd = unsafe { passwd.assume_init() };
		// SAFETY: A successful lookup returned a NUL-terminated string backed by the live buffer.
		let name = unsafe { CStr::from_ptr(passwd.pw_name) }
			.to_string_lossy()
			.into_owned();
		// SAFETY: A successful lookup returned a NUL-terminated string backed by the live buffer.
		let home = unsafe { CStr::from_ptr(passwd.pw_dir) };
		checkpoint();
		let home = home.to_string_lossy().into_owned();
		let user = User {
			gid: passwd.pw_gid,
			home: PathBuf::from(home),
			name,
			uid: passwd.pw_uid,
		};

		return Ok(user);
	}
}

#[cfg(test)]
mod tests {
	use {super::*, std::sync::Arc};

	#[test]
	fn concurrent_user_resolution_owns_the_passwd_record() {
		let root = UserQuery::Uid(0);
		let expected = resolve_user_with_checkpoint(&root, || {}).unwrap();
		let checkpoint = Arc::new(std::sync::Barrier::new(2));
		let task = {
			let checkpoint = checkpoint.clone();
			std::thread::spawn(move || {
				resolve_user_with_checkpoint(&root, || {
					checkpoint.wait();
					checkpoint.wait();
				})
				.unwrap()
			})
		};
		checkpoint.wait();
		resolve_user_with_checkpoint(&UserQuery::Uid(2), || {}).unwrap();
		checkpoint.wait();
		let user = task.join().unwrap();

		assert_eq!(user, expected);
	}
}
