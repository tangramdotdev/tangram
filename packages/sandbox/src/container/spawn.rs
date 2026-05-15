use {
	crate::Sandbox,
	indoc::indoc,
	std::{
		ffi::{CStr, CString, OsStr},
		fmt::Write as _,
		net::Ipv4Addr,
		os::{fd::AsRawFd as _, unix::ffi::OsStrExt as _},
		path::{Path, PathBuf},
		sync::Arc,
	},
	tangram_client::prelude::*,
};

struct User {
	gid: libc::gid_t,
	home: PathBuf,
	name: String,
	uid: libc::uid_t,
}

pub(crate) async fn spawn(
	arg: &crate::Arg,
	serve_arg: &crate::serve::Arg,
	network: Option<&mut crate::network::Network>,
	helper: Option<&Arc<crate::rootless::client::Helper>>,
) -> tg::Result<crate::process::ProcessHandle> {
	let crate::Isolation::Container(_) = &arg.isolation else {
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
			let id = arg.id.clone();
			let bridge_name = network.bridge_name().to_owned();
			Some(
				tokio::task::spawn_blocking(move || {
					crate::network::veth::Pair::new(&id, &bridge_name)
				})
				.await
				.map_err(|error| tg::error!(!error, "the veth creation task panicked"))??,
			)
		},
		_ => None,
	};

	// The helper-driven pasta path: when the helper is present and we resolved
	// to a Pasta network, the wrapper is forked from inside the helper's user
	// namespace and placed in a per-sandbox network namespace by `setns`. The
	// existing `unshare(CLONE_NEWUSER)`/`unshare(CLONE_NEWNET)` and pasta-side
	// `--pid <wrapper_pid>` attach are bypassed.
	let helper_pasta = matches!(network.as_deref(), Some(crate::network::Network::Pasta(_)))
		.then(|| helper)
		.flatten();

	prepare_sandbox_directory(&arg.path)?;
	let user = prepare_etc_files(&arg.path, network.as_deref(), arg.user.as_deref(), &arg.dns)?;
	let upper_path = Sandbox::host_upper_path_from_root(&arg.path);
	for mount in &arg.mounts {
		crate::root::ensure_mount_target(&arg.rootfs_path, &upper_path, mount)?;
	}
	let stdio = matches!(serve_arg.url.scheme(), Some("http+stdio"));
	let init_arg = super::init::Arg {
		serve: serve_arg.clone(),
	};

	// In the helper-driven path the wrapper inherits the helper's
	// multi-uid user namespace, where in-ns uid 0 maps to the host uid that
	// owns the bind-mounted artifacts and listen socket. The wrapper must
	// stay at in-ns uid 0 so that host-visible accesses use the host uid
	// rather than a subordinate uid that lacks DAC on those host paths.
	let (effective_uid, effective_gid) = if helper_pasta.is_some() {
		(0u32, 0u32)
	} else {
		(user.uid, user.gid)
	};

	// Build the wrapper argv as a flat `Vec<String>` so we can either spawn it
	// directly or send it to the helper for forking.
	let mut argv: Vec<String> = Vec::new();
	argv.push(path_to_string(&arg.tangram_path, "tangram_path")?);
	argv.extend(["sandbox".into(), "container".into(), "run".into()]);
	push_kv(&mut argv, "--id", arg.id.to_string());
	argv.push("--unshare-all".into());
	argv.push("--as-pid-1".into());
	argv.push("--die-with-parent".into());
	argv.push("--new-session".into());
	push_kv(&mut argv, "--uid", effective_uid.to_string());
	push_kv(&mut argv, "--gid", effective_gid.to_string());
	push_kv(&mut argv, "--nice", arg.nice.to_string());
	push_kv(&mut argv, "--chdir", "/");
	push_kv(
		&mut argv,
		"--overlay-src",
		path_to_string(&arg.rootfs_path, "rootfs_path")?,
	);
	argv.push("--overlay".into());
	argv.push(path_to_string(
		&Sandbox::host_upper_path_from_root(&arg.path),
		"upper path",
	)?);
	argv.push(path_to_string(
		&Sandbox::host_work_path_from_root(&arg.path),
		"work path",
	)?);
	argv.push("/".into());
	push_kv(&mut argv, "--dev", "/dev");
	push_kv(&mut argv, "--proc", "/proc");
	argv.push("--bind".into());
	argv.push(path_to_string(
		&Sandbox::host_tmp_path_from_root(&arg.path),
		"host tmp",
	)?);
	argv.push(path_to_string(
		&Sandbox::guest_tmp_path_from_root(&arg.path),
		"guest tmp",
	)?);
	if let Some(network_arg) = &network_arg {
		push_kv(&mut argv, "--network", network_arg.clone());
	}
	if helper_pasta.is_some() {
		// The helper has already placed the wrapper in the desired user and
		// network namespaces; bypass the redundant wrapper-side unshares.
		argv.push("--skip-user-ns".into());
		argv.push("--skip-netns".into());
	} else {
		if let Some(crate::network::Network::Pasta(network)) = network.as_deref()
			&& let Some(guest_pipe) = network.guest_pipe()
		{
			push_kv(
				&mut argv,
				"--network-fd",
				guest_pipe.as_raw_fd().to_string(),
			);
		}
		if let Some(veth) = veth.as_ref() {
			push_kv(
				&mut argv,
				"--network-fd",
				veth.guest_pipe.as_ref().unwrap().as_raw_fd().to_string(),
			);
		}
	}
	if let Some(crate::network::Network::Veth(network)) = network.as_deref() {
		push_kv(&mut argv, "--gateway-ip", network.gateway_ip().to_string());
		push_kv(&mut argv, "--guest-ip", network.guest_ip().to_string());
	}
	if let Some(hostname) = &arg.hostname {
		push_kv(&mut argv, "--hostname", hostname.clone());
	}
	if let Some(cpu) = arg.cpu {
		push_kv(&mut argv, "--cgroup-cpu", cpu.to_string());
	}
	if let Some(memory) = arg.memory {
		push_kv(&mut argv, "--cgroup-memory", memory.to_string());
	}
	argv.push("--setenv".into());
	argv.push("HOME".into());
	argv.push(path_to_string(&user.home, "user home")?);
	argv.push("--setenv".into());
	argv.push("SSL_CERT_DIR".into());
	argv.push(path_to_string(
		&Sandbox::guest_ssl_cert_dir(),
		"ssl cert dir",
	)?);
	argv.push("--ro-bind".into());
	argv.push(path_to_string(
		&Sandbox::host_passwd_path_from_root(&arg.path),
		"passwd",
	)?);
	argv.push("/etc/passwd".into());
	argv.push("--ro-bind".into());
	argv.push(path_to_string(
		&Sandbox::host_nsswitch_path_from_root(&arg.path),
		"nsswitch",
	)?);
	argv.push("/etc/nsswitch.conf".into());
	argv.push("--ro-bind".into());
	argv.push(path_to_string(&arg.artifacts_path, "artifacts host")?);
	argv.push(path_to_string(
		&Sandbox::guest_artifacts_path_from_host_artifacts_path(&arg.artifacts_path),
		"artifacts guest",
	)?);
	argv.push("--ro-bind".into());
	argv.push(path_to_string(&arg.tangram_path, "tangram_path")?);
	argv.push(path_to_string(
		&Sandbox::guest_libexec_tangram_path(),
		"libexec tangram",
	)?);
	argv.push("--bind".into());
	argv.push(path_to_string(
		&Sandbox::host_tangram_socket_path_from_root(&arg.path),
		"host socket",
	)?);
	argv.push(path_to_string(
		&Sandbox::guest_tangram_socket_path_from_root(&arg.path),
		"guest socket",
	)?);
	argv.push("--bind".into());
	argv.push(path_to_string(
		&Sandbox::host_output_path_from_root(&arg.path),
		"host output",
	)?);
	argv.push(path_to_string(
		&Sandbox::guest_output_path_from_root(&arg.path),
		"guest output",
	)?);
	if !stdio {
		argv.push("--bind".into());
		argv.push(path_to_string(
			&Sandbox::host_listen_path_from_root(&arg.path),
			"host listen",
		)?);
		argv.push(path_to_string(
			&Sandbox::guest_listen_path_from_root(&arg.path),
			"guest listen",
		)?);
	}
	let resolv_conf_path = Sandbox::host_resolv_conf_path_from_root(&arg.path);
	if resolv_conf_path.exists() {
		argv.push("--ro-bind".into());
		argv.push(path_to_string(&resolv_conf_path, "resolv.conf")?);
		argv.push("/etc/resolv.conf".into());
	}
	for mount in &arg.mounts {
		argv.push(
			if mount.readonly {
				"--ro-bind"
			} else {
				"--bind"
			}
			.into(),
		);
		argv.push(path_to_string(&mount.source, "mount source")?);
		argv.push(path_to_string(&mount.target, "mount target")?);
	}
	let cgroup_name = arg
		.path
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("sandbox")
		.to_owned();
	push_kv(&mut argv, "--cgroup", cgroup_name);
	argv.push("--cgroup-memory-oom-group".into());
	argv.push("--".into());
	argv.push(path_to_string(
		&Sandbox::guest_tangram_path_from_host_tangram_path(&arg.tangram_path),
		"guest tangram",
	)?);
	argv.extend([
		"sandbox".into(),
		"container".into(),
		"init".into(),
		"--output-path".into(),
		path_to_string(&init_arg.serve.output_path, "init output path")?,
		"--url".into(),
		init_arg.serve.url.to_string(),
		"--tangram-path".into(),
		path_to_string(&init_arg.serve.tangram_path, "init tangram-path")?,
	]);
	for path in &init_arg.serve.library_paths {
		argv.push("--library-path".into());
		argv.push(path_to_string(path, "library path")?);
	}

	// Helper-driven path.
	if let Some(helper) = helper_pasta {
		return spawn_via_helper(arg, helper, network.unwrap(), &argv).await;
	}

	// Direct path: spawn the wrapper as a tokio child.
	let mut command = tokio::process::Command::new(&argv[0]);
	command.args(&argv[1..]);
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
		let pid = i32::try_from(pid).map_err(|source| tg::error!(!source, "invalid child pid"))?;
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
	Ok(crate::process::ProcessHandle::Direct(child))
}

/// Take the helper-driven path: ask the helper to create a per-sandbox netns,
/// fork pasta against the netns path, then fork the wrapper into the helper's
/// user namespace and `setns` it into the netns. Returns a `ProcessHandle`
/// that holds a pidfd on the wrapper and a reference to the helper for
/// cleanup.
async fn spawn_via_helper(
	arg: &crate::Arg,
	helper: &Arc<crate::rootless::client::Helper>,
	network: &mut crate::network::Network,
	wrapper_argv: &[String],
) -> tg::Result<crate::process::ProcessHandle> {
	let crate::network::Network::Pasta(pasta) = network else {
		return Err(tg::error!(
			"the helper-driven path requires pasta networking"
		));
	};

	let sandbox_id = arg.id.to_string();

	// Create the netns. The helper bind-mounts the holder's
	// `/proc/self/ns/net` to a path inside its mount namespace and returns
	// that path.
	let helper_clone = helper.clone();
	let id_clone = sandbox_id.clone();
	let netns_path = tokio::task::spawn_blocking(move || helper_clone.spawn_netns(&id_clone))
		.await
		.map_err(|error| tg::error!(!error, "the helper spawn_netns task panicked"))??;

	// Build the pasta argv pointed at the netns path and ask the helper to
	// fork it.
	let pasta_argv = pasta.build_argv_for_netns(&netns_path)?;
	let helper_clone = helper.clone();
	let id_clone = sandbox_id.clone();
	tokio::task::spawn_blocking(move || helper_clone.spawn_pasta(&id_clone, &pasta_argv))
		.await
		.map_err(|error| tg::error!(!error, "the helper spawn_pasta task panicked"))??;

	// Wait for pasta to write its pid file before forking the wrapper. This
	// matches the direct-path ordering.
	pasta.wait_for_pid_file().await?;

	// Fork the wrapper from inside the helper. It enters the netns via `setns`
	// before exec; the helper holds CAP_SYS_ADMIN in the user ns that owns
	// the netns, so `setns(CLONE_NEWNET)` succeeds.
	let helper_clone = helper.clone();
	let id_clone = sandbox_id.clone();
	let argv_clone = wrapper_argv.to_vec();
	let wrapper_pid =
		tokio::task::spawn_blocking(move || helper_clone.spawn_wrapper(&id_clone, &argv_clone))
			.await
			.map_err(|error| tg::error!(!error, "the helper spawn_wrapper task panicked"))??;

	let helper_process =
		crate::process::HelperProcess::new(helper.clone(), sandbox_id, wrapper_pid)?;
	Ok(crate::process::ProcessHandle::Helper(helper_process))
}

fn path_to_string(path: &Path, context: &str) -> tg::Result<String> {
	path.to_str()
		.map(str::to_owned)
		.ok_or_else(|| tg::error!(path = %path.display(), "the {context} is not valid utf-8"))
}

fn push_kv<V: Into<String>>(argv: &mut Vec<String>, flag: &str, value: V) {
	argv.push(flag.to_owned());
	argv.push(value.into());
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
	user: Option<&str>,
	dns: &[Ipv4Addr],
) -> tg::Result<User> {
	let user = resolve_user(user)?;
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
			let contents = if dns.is_empty() {
				String::new()
			} else {
				format!("nameserver {}\n", crate::network::pasta::DNS_FORWARD_IP)
			};
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
	let ptr = unsafe {
		if let Some(name) = name {
			let name = CString::new(OsStr::new(name).as_bytes())
				.map_err(|error| tg::error!(!error, "failed to encode the user name"))?;
			libc::getpwnam(name.as_ptr())
		} else {
			libc::getpwuid(libc::getuid())
		}
	};
	if ptr.is_null() {
		return Err(tg::error!("failed to resolve the user"));
	}
	let passwd = unsafe { &*ptr };
	let name = unsafe { CStr::from_ptr(passwd.pw_name) }
		.to_string_lossy()
		.into_owned();
	let home = unsafe { CStr::from_ptr(passwd.pw_dir) }
		.to_string_lossy()
		.into_owned();
	let user = User {
		gid: passwd.pw_gid,
		home: PathBuf::from(home),
		name,
		uid: passwd.pw_uid,
	};
	Ok(user)
}
