use {
	crate::Sandbox,
	indoc::indoc,
	std::{
		ffi::{CStr, CString, OsStr},
		fmt::Write as _,
		mem::MaybeUninit,
		net::Ipv4Addr,
		os::{
			fd::AsRawFd as _,
			unix::{ffi::OsStrExt as _, net::UnixStream},
		},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
	tokio::io::AsyncReadExt as _,
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

	#[cfg(test)]
	Record(User),

	Uid(libc::uid_t),
}

pub(crate) struct Output {
	pub filesystem: Option<crate::filesystem::Filesystem>,
	pub process: tokio::process::Child,
	pub storage: Option<crate::budget::Allocation>,
}

pub(crate) async fn spawn(
	arg: &crate::Arg,
	serve_arg: &crate::serve::Arg,
	network: Option<&mut crate::network::Network>,
	cgroup: &crate::cgroup::Cgroup,
) -> tg::Result<Output> {
	// Prepare the network pair.
	let isolation = &arg.resources;
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

	// Allocate the filesystem resources.
	let filesystem_inodes = isolation.max_filesystem_inodes;
	let storage = match (arg.storage, &arg.storage_pool) {
		(Some(resource), Some(pool)) => Some(pool.allocate(resource)?),
		(Some(_), None) => return Err(tg::error!("storage limits require a runner storage pool")),
		(None, _) => None,
	};
	let filesystem_size = storage
		.as_ref()
		.map(crate::budget::Allocation::limit)
		.or(isolation.max_filesystem_size);
	let filesystem_limited = filesystem_inodes.is_some() || filesystem_size.is_some();
	let filesystem_separate = filesystem_limited || isolation.uid_map.is_some();
	let filesystem_path = if filesystem_separate {
		Sandbox::host_filesystem_path_from_root(&arg.path)
	} else {
		arg.path.clone()
	};
	if filesystem_limited {
		let project = isolation.filesystem_project_id.ok_or_else(|| {
			tg::error!("missing a project ID for the limited container filesystem")
		})?;
		crate::filesystem::create(
			&filesystem_path,
			project,
			filesystem_size,
			filesystem_inodes,
		)?;
	} else if filesystem_separate {
		crate::filesystem::create_directory(&filesystem_path)?;
	}
	if let Some(storage) = &storage {
		let project = isolation.filesystem_project_id.unwrap();
		let control = crate::filesystem::control(&filesystem_path, project)?;
		storage.bind(control)?;
	}

	// Prepare the sandbox directories and identity.
	prepare_sandbox_directory(
		&arg.path,
		&filesystem_path,
		filesystem_separate,
		isolation.uid_map.is_some(),
	)?;
	let user = prepare_etc_files(&arg.path, network.as_deref(), &arg.dns)?;
	let user_namespace = prepare_user_namespace(isolation, &user)?;

	// Open the runner's view, so that its files keep their host ownership during cleanup.
	let host_filesystem = if user_namespace.is_some() {
		let fd = crate::filesystem::open(&filesystem_path).map_err(|error| {
			tg::error!(
				!error,
				"subordinate identities require CAP_SYS_ADMIN in the backing filesystem's user namespace"
			)
		})?;
		Some(crate::filesystem::Filesystem::new(fd))
	} else {
		None
	};

	// Create the filesystem control socket.
	let (filesystem_sendfd, filesystem_recvfd) = if filesystem_separate {
		let (sendfd, recvfd) = rustix::net::socketpair(
			rustix::net::AddressFamily::UNIX,
			rustix::net::SocketType::STREAM,
			rustix::net::SocketFlags::CLOEXEC,
			None,
		)
		.map_err(|error| tg::error!(!error, "failed to create the filesystem socket pair"))?;
		(Some(sendfd), Some(recvfd))
	} else {
		(None, None)
	};

	// Describe the prepared Tangram filesystem and workload independently of the launcher's CLI.
	let mut mounts = crate::mount::Arg {
		nodev: true,
		nosuid: true,
		overlay_sources: vec![arg.rootfs_path.clone()],
		overlays: vec![crate::mount::Overlay {
			target: "/".into(),
			upperdir: Sandbox::host_upper_path_from_root(&filesystem_path),
			workdir: Sandbox::host_work_path_from_root(&filesystem_path),
		}],
		..Default::default()
	};
	let mut add_mount = |source: PathBuf, target: PathBuf, readonly: bool| {
		let mount = crate::mount::Bind {
			devices: false,
			fd: None,
			source,
			target,
		};
		if readonly {
			mounts.ro_binds.push(mount);
		} else {
			mounts.binds.push(mount);
		}
	};
	add_mount(
		Sandbox::host_tmp_path_from_root(&filesystem_path),
		Sandbox::guest_tmp_path_from_root(&arg.path),
		false,
	);
	add_mount(
		Sandbox::host_passwd_path_from_root(&arg.path),
		"/etc/passwd".into(),
		true,
	);
	add_mount(
		Sandbox::host_nsswitch_path_from_root(&arg.path),
		"/etc/nsswitch.conf".into(),
		true,
	);
	add_mount(
		arg.store_path.clone(),
		Sandbox::guest_store_path_from_host_store_path(&arg.store_path),
		true,
	);
	add_mount(
		arg.tangram_path.clone(),
		Sandbox::guest_libexec_tangram_path(),
		true,
	);
	add_mount(
		arg.tangram_socket_path
			.clone()
			.ok_or_else(|| tg::error!("missing the tangram socket path"))?,
		Sandbox::guest_tangram_socket_path_from_root(&arg.path),
		false,
	);
	add_mount(
		Sandbox::host_output_path_from_root(&filesystem_path),
		Sandbox::guest_output_path_from_root(&arg.path),
		false,
	);
	if !matches!(serve_arg.url.scheme(), Some("http+stdio")) {
		add_mount(
			Sandbox::host_listen_path_from_root(&arg.path),
			Sandbox::guest_listen_path_from_root(&arg.path),
			false,
		);
	}

	// Bind the resolver configuration and user mounts.
	let resolv_conf_path = Sandbox::host_resolv_conf_path_from_root(&arg.path);
	if resolv_conf_path.exists() {
		add_mount(resolv_conf_path, "/etc/resolv.conf".into(), true);
	}
	for mount in &arg.mounts {
		add_mount(mount.source.clone(), mount.target.clone(), mount.readonly);
	}

	// Create the network launch configuration.
	let network_arg = match network.as_deref() {
		None => None,
		Some(crate::network::Network::Host) => Some(crate::run::Network::Host),
		Some(crate::network::Network::Pasta(network)) => Some(crate::run::Network::Pasta {
			fd: network
				.guest_pipe()
				.ok_or_else(|| tg::error!("missing the pasta synchronization descriptor"))?
				.as_raw_fd(),
		}),
		Some(crate::network::Network::Veth(network)) => Some(crate::run::Network::Veth {
			fd: veth
				.as_ref()
				.unwrap()
				.guest_pipe
				.as_ref()
				.unwrap()
				.as_raw_fd(),
			gateway: network.gateway_ip(),
			guest: network.guest_ip(),
		}),
		Some(crate::network::Network::Passt(_) | crate::network::Network::Tap(_)) => {
			return Err(tg::error!("unsupported Linux setup network"));
		},
	};

	// Create the sandbox init command.
	let mut arguments = vec![
		Sandbox::guest_tangram_path_from_host_tangram_path(&arg.tangram_path).into_os_string(),
		"sandbox".into(),
		"init".into(),
		"--backend".into(),
		arg.backend.kind().name().into(),
		"--output-path".into(),
		serve_arg.output_path.clone().into_os_string(),
		"--url".into(),
		serve_arg.url.to_string().into(),
		"--tangram-path".into(),
		serve_arg.tangram_path.clone().into_os_string(),
	];
	arguments.extend(
		crate::util::http2_args(&serve_arg.http2)
			.into_iter()
			.map(Into::into),
	);
	for path in &serve_arg.library_paths {
		arguments.extend(["--library-path".into(), path.clone().into_os_string()]);
	}

	// Create the process environment.
	let environment = std::collections::BTreeMap::from([
		("HOME".into(), user.home.clone().into_os_string()),
		(
			"SSL_CERT_DIR".into(),
			Sandbox::guest_ssl_cert_dir().into_os_string(),
		),
	]);

	// Encode the process launch configuration.
	let arguments = arguments
		.into_iter()
		.map(|argument: std::ffi::OsString| {
			argument
				.into_string()
				.map_err(|_| tg::error!("sandbox launch arguments must be valid UTF-8"))
		})
		.collect::<tg::Result<Vec<_>>>()?;
	let environment = environment
		.into_iter()
		.map(|(key, value): (std::ffi::OsString, std::ffi::OsString)| {
			let key = key
				.into_string()
				.map_err(|_| tg::error!("sandbox launch environment names must be valid UTF-8"))?;
			let value = value
				.into_string()
				.map_err(|_| tg::error!("sandbox launch environment values must be valid UTF-8"))?;
			Ok((key, value))
		})
		.collect::<tg::Result<_>>()?;
	let command_arg = crate::run::Command {
		args: arguments,
		cwd: "/".into(),
		env: environment,
	};
	let cgroup_readonly = isolation.cgroup_readonly || arg.cpu_pool.is_some();
	let options = crate::run::Options {
		cgroup_delegate: !cgroup_readonly,
		cgroup_readonly,
		filesystem_fd: filesystem_sendfd
			.as_ref()
			.map(std::os::fd::AsRawFd::as_raw_fd),
		filesystem_path: filesystem_separate.then_some(filesystem_path.clone()),
		fuse_fd: arg.fuse_fd.as_ref().map(std::os::fd::AsRawFd::as_raw_fd),
		fuse_path: arg.fuse_fd.as_ref().map(|_| arg.store_path.clone()),
		index: arg.index,
		mounts,
		network: network_arg,
		nice: arg.nice,
		rlimit_nofile: isolation.max_open_files,
		user_namespace_fd: user_namespace
			.as_ref()
			.map(|(_, guest, _, _)| guest.as_raw_fd()),
		..Default::default()
	};

	// Select the backend isolation inputs.
	let backend = match &arg.backend {
		crate::Backend::Builtin => crate::run::Backend::Builtin(crate::builtin::run::Arg {
			as_pid_1: true,
			gid: user.gid,
			hostname: arg.hostname.clone(),
			root: None,
			seccomp: isolation.seccomp,
			uid: user.uid,
		}),
		crate::Backend::Bubblewrap { path } => {
			crate::run::Backend::Bubblewrap(crate::bubblewrap::run::Arg {
				as_pid_1: true,
				gid: user.gid,
				hostname: arg.hostname.clone(),
				path: path.clone(),
				root: None,
				seccomp: isolation.seccomp,
				uid: user.uid,
			})
		},
		crate::Backend::Gvisor { path, platform } => {
			crate::run::Backend::Gvisor(crate::gvisor::run::Arg {
				gid: user.gid,
				hostname: arg.hostname.clone(),
				path: path.clone(),
				platform: *platform,
				uid: user.uid,
				..Default::default()
			})
		},
		crate::Backend::CloudHypervisor(_) | crate::Backend::Seatbelt => unreachable!(),
	};
	let launch = crate::run::Arg {
		backend,
		command: command_arg,
		options,
	};

	// Select the launcher cgroup, creating a guest cgroup with the enabled controllers when the guest can create cgroups.
	let launcher_cgroup = if launch.options.cgroup_delegate {
		let handle = cgroup.handle()?;
		handle.enable_controllers()?;
		cgroup.child(crate::cgroup::GUEST)?
	} else {
		cgroup.handle()?
	};

	// Create the launcher and inherit the control descriptors.
	let mut command = tokio::process::Command::new(&arg.tangram_path);
	let config = crate::run::configure_command(&mut command, &launch)?;
	for fd in [
		launch.options.filesystem_fd,
		launch.options.fuse_fd,
		launch.options.user_namespace_fd,
	]
	.into_iter()
	.flatten()
	{
		inherit_fd(&mut command, fd);
	}
	match launch.options.network {
		Some(crate::run::Network::Pasta { fd } | crate::run::Network::Veth { fd, .. }) => {
			inherit_fd(&mut command, fd);
		},
		None | Some(crate::run::Network::Host) => {},
	}

	// Keep the launcher outside the server's terminal process group during Ctrl-C shutdown.
	command
		.kill_on_drop(true)
		.process_group(0)
		.stdin(std::process::Stdio::piped())
		.stdout(std::process::Stdio::piped())
		.stderr(std::process::Stdio::inherit());

	// Spawn the launcher and install the identity maps.
	let child = spawn_command(&mut command, &launcher_cgroup)?;
	config.finish().await?;
	if let Some((host, guest, uid_map, gid_map)) = user_namespace {
		drop(guest);

		// Map the overlay lower layers too, so that copy-up preserves the runner's on-disk ownership.
		let paths = std::iter::once(&filesystem_path)
			.chain(&launch.options.mounts.overlay_sources)
			.collect::<Vec<_>>();
		configure_user_namespace(host, &uid_map, &gid_map, &paths).await?;
	}

	// Complete the network setup.
	drop(filesystem_sendfd);
	if let Some(crate::network::Network::Pasta(network)) = network {
		network.take_guest_pipe();
		let pid = child
			.id()
			.ok_or_else(|| tg::error!("no child pid available"))?;
		let pid = i32::try_from(pid).map_err(|error| tg::error!(!error, "invalid child pid"))?;
		let pasta_cgroup = if launch.options.cgroup_delegate {
			cgroup.child(crate::cgroup::SIDECARS)?
		} else {
			cgroup.handle()?
		};
		network.start_netns(pid, &pasta_cgroup).await?;
	}
	if let Some(veth) = veth.as_mut() {
		drop(veth.guest_pipe.take());
		let pid = child
			.id()
			.ok_or_else(|| tg::error!("no child pid available"))?;
		let pid = i32::try_from(pid).map_err(|error| tg::error!(!error, "invalid child pid"))?;
		veth.connect(pid).await?;
	}

	// The child prepares its filesystem only after the network handshake completes.
	let filesystem = match filesystem_recvfd {
		Some(fd) => Some(crate::filesystem::Filesystem::new(
			tokio::task::spawn_blocking(move || crate::filesystem::receive(&fd))
				.await
				.map_err(|error| tg::error!(!error, "the filesystem task panicked"))??,
		)),
		None => None,
	};
	let filesystem = host_filesystem.or(filesystem);

	// Create the launch output.
	let output = Output {
		filesystem,
		process: child,
		storage,
	};

	Ok(output)
}

pub(crate) fn spawn_command(
	command: &mut tokio::process::Command,
	cgroup: &crate::cgroup::Handle,
) -> tg::Result<tokio::process::Child> {
	let directory = cgroup.open_fd()?;
	crate::cgroup::enter_before_exec(command.as_std_mut(), directory.as_raw_fd());
	let child = command
		.spawn()
		.map_err(|error| tg::error!(!error, "failed to spawn the sandbox launcher"))?;
	Ok(child)
}

fn prepare_sandbox_directory(
	sandbox_path: &Path,
	filesystem_path: &Path,
	filesystem_separate: bool,
	mapped_identity: bool,
) -> tg::Result<()> {
	// Create the sandbox directories.
	let mut paths = vec![Sandbox::host_etc_path_from_root(sandbox_path)];
	if !filesystem_separate {
		paths.extend([
			Sandbox::host_output_path_from_root(filesystem_path),
			Sandbox::host_scratch_path_from_root(filesystem_path),
			Sandbox::host_tmp_path_from_root(filesystem_path),
			Sandbox::host_upper_path_from_root(filesystem_path),
			Sandbox::host_work_path_from_root(filesystem_path),
		]);
	}
	for path in paths {
		std::fs::create_dir_all(&path).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the sandbox path"),
		)?;
	}

	// Set the filesystem directory permissions.
	let permissions =
		<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o1777);
	if !filesystem_separate {
		let tmp_path = Sandbox::host_tmp_path_from_root(filesystem_path);
		std::fs::set_permissions(&tmp_path, permissions).map_err(|error| {
			tg::error!(
				!error,
				path = %tmp_path.display(),
				"failed to set sandbox path permissions"
			)
		})?;
		let upper_path = Sandbox::host_upper_path_from_root(filesystem_path);
		let tangram_path = upper_path.join("opt/tangram");
		std::fs::create_dir_all(&tangram_path).map_err(|error| {
			tg::error!(
				!error,
				path = %tangram_path.display(),
				"failed to create the sandbox path"
			)
		})?;
		if mapped_identity {
			for path in [
				Sandbox::host_output_path_from_root(filesystem_path),
				Sandbox::host_scratch_path_from_root(filesystem_path),
				Sandbox::host_upper_path_from_root(filesystem_path),
				Sandbox::host_work_path_from_root(filesystem_path),
				tangram_path,
			] {
				let permissions =
					<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o777);
				std::fs::set_permissions(&path, permissions).map_err(|error| {
					tg::error!(
						!error,
						path = %path.display(),
						"failed to set mapped sandbox path permissions"
					)
				})?;
			}
		}
	}

	Ok(())
}

fn prepare_user_namespace(
	isolation: &crate::Resources,
	user: &User,
) -> tg::Result<Option<(UnixStream, UnixStream, crate::IdMap, crate::IdMap)>> {
	// Validate the subordinate identity maps.
	let (Some(uid_map), Some(gid_map)) = (&isolation.uid_map, &isolation.gid_map) else {
		if isolation.uid_map.is_some() || isolation.gid_map.is_some() {
			return Err(tg::error!(
				"container uid and gid maps must be configured together"
			));
		}
		return Ok(None);
	};
	if user.uid >= uid_map.count {
		return Err(tg::error!(
			count = %uid_map.count,
			uid = %user.uid,
			"the container uid map does not contain the workload uid"
		));
	}
	if user.gid >= gid_map.count {
		return Err(tg::error!(
			count = %gid_map.count,
			gid = %user.gid,
			"the container gid map does not contain the workload gid"
		));
	}

	// Create the namespace synchronization socket.
	let (host, guest) = UnixStream::pair()
		.map_err(|error| tg::error!(!error, "failed to create a user namespace socket pair"))?;
	let output = Some((host, guest, uid_map.clone(), gid_map.clone()));

	Ok(output)
}

fn inherit_fd(command: &mut tokio::process::Command, raw: libc::c_int) {
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

async fn configure_user_namespace(
	socket: UnixStream,
	uid_map: &crate::IdMap,
	gid_map: &crate::IdMap,
	paths: &[&PathBuf],
) -> tg::Result<()> {
	socket
		.set_nonblocking(true)
		.map_err(|error| tg::error!(!error, "failed to configure the identity socket"))?;
	let mut socket = tokio::net::UnixStream::from_std(socket)
		.map_err(|error| tg::error!(!error, "failed to register the identity socket"))?;
	let mut buffer = [0; std::mem::size_of::<libc::pid_t>()];
	socket
		.read_exact(&mut buffer)
		.await
		.map_err(|error| tg::error!(!error, "failed to wait for the user namespace"))?;
	let pid = libc::pid_t::from_ne_bytes(buffer);

	// SAFETY: This function has no preconditions.
	let setup_uid = unsafe { libc::geteuid() };

	// SAFETY: This function has no preconditions.
	let setup_gid = unsafe { libc::getegid() };
	if setup_uid == 0 {
		write_id_map(pid, "uid_map", uid_map, setup_uid)?;
		write_id_map(pid, "gid_map", gid_map, setup_gid)?;
	} else {
		run_id_map_helper(pid, uid_map, setup_uid, "uid").await?;
		run_id_map_helper(pid, gid_map, setup_gid, "gid").await?;
	}

	// Send the ID-mapped mounts, which also releases the user namespace.
	let namespace = std::fs::File::open(format!("/proc/{pid}/ns/user"))
		.map_err(|error| tg::error!(!error, "failed to open the workload user namespace"))?
		.into();
	let socket = socket
		.into_std()
		.map_err(|error| tg::error!(!error, "failed to deregister the identity socket"))?;
	socket
		.set_nonblocking(false)
		.map_err(|error| tg::error!(!error, "failed to configure the identity socket"))?;
	let socket = socket.into();
	for path in paths {
		let filesystem = crate::filesystem::open_mapped(path, &namespace)?;
		crate::filesystem::send(&socket, &filesystem)?;
	}

	Ok(())
}

fn write_id_map(pid: libc::pid_t, name: &str, map: &crate::IdMap, setup: u32) -> tg::Result<()> {
	let path = PathBuf::from(format!("/proc/{pid}/{name}"));
	let contents = format!("0 {} {}\n{} {} 1\n", map.host, map.count, map.count, setup);
	std::fs::write(&path, contents).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to write the user namespace map"),
	)?;
	Ok(())
}

async fn run_id_map_helper(
	pid: libc::pid_t,
	map: &crate::IdMap,
	setup: u32,
	kind: &str,
) -> tg::Result<()> {
	let output = tokio::process::Command::new(&map.helper)
		.kill_on_drop(true)
		.arg(pid.to_string())
		.arg("0")
		.arg(map.host.to_string())
		.arg(map.count.to_string())
		.arg(map.count.to_string())
		.arg(setup.to_string())
		.arg("1")
		.output()
		.await
		.map_err(|error| {
			tg::error!(
				!error,
				path = %map.helper.display(),
				"failed to run the {kind} map helper"
			)
		})?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
		return Err(tg::error!(
			%stderr,
			status = %output.status,
			"the {kind} map helper failed"
		));
	}
	Ok(())
}

fn prepare_etc_files(
	sandbox_path: &Path,
	network: Option<&crate::network::Network>,
	dns: &[Ipv4Addr],
) -> tg::Result<User> {
	// Write the passwd file.
	let user = resolve_user(None)?;
	let passwd = render_passwd(&user);
	std::fs::write(Sandbox::host_passwd_path_from_root(sandbox_path), passwd)
		.map_err(|error| tg::error!(!error, "failed to write /etc/passwd"))?;

	// Write the name service configuration.
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

	// Write the resolver configuration.
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

				#[cfg(test)]
				UserQuery::Record(user) => tests::resolve_record(user, &mut passwd, &mut buffer, &mut result),

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
		// Create the passwd records.
		let expected = User {
			gid: 0,
			home: PathBuf::from("/root"),
			name: "root".to_owned(),
			uid: 0,
		};
		let root = UserQuery::Record(User {
			gid: expected.gid,
			home: expected.home.clone(),
			name: expected.name.clone(),
			uid: expected.uid,
		});
		let bin = UserQuery::Record(User {
			gid: 2,
			home: PathBuf::from("/bin"),
			name: "bin".to_owned(),
			uid: 2,
		});

		// Resolve the records concurrently.
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
		resolve_user_with_checkpoint(&bin, || {}).unwrap();
		checkpoint.wait();

		// Verify the first result retains its own record.
		let user = task.join().unwrap();

		assert_eq!(user, expected);
	}

	pub(super) fn resolve_record(
		user: &User,
		passwd: &mut MaybeUninit<libc::passwd>,
		buffer: &mut [u8],
		result: &mut *mut libc::passwd,
	) -> libc::c_int {
		let name = CString::new(user.name.as_bytes()).unwrap();
		let home = CString::new(user.home.as_os_str().as_bytes()).unwrap();
		let name = name.as_bytes_with_nul();
		let home = home.as_bytes_with_nul();
		if buffer.len() < name.len() + home.len() {
			*result = std::ptr::null_mut();
			return libc::ERANGE;
		}
		let (name_buffer, home_buffer) = buffer.split_at_mut(name.len());
		name_buffer.copy_from_slice(name);
		home_buffer[..home.len()].copy_from_slice(home);
		let record = libc::passwd {
			pw_dir: home_buffer.as_mut_ptr().cast(),
			pw_gecos: std::ptr::null_mut(),
			pw_gid: user.gid,
			pw_name: name_buffer.as_mut_ptr().cast(),
			pw_passwd: std::ptr::null_mut(),
			pw_shell: std::ptr::null_mut(),
			pw_uid: user.uid,
		};
		*result = passwd.write(record);
		0
	}
}
