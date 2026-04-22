use {
	crate::Sandbox,
	indoc::indoc,
	std::{
		ffi::{CStr, CString, OsStr},
		fmt::Write as _,
		os::unix::ffi::OsStrExt as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

struct User {
	gid: libc::gid_t,
	home: PathBuf,
	name: String,
	uid: libc::uid_t,
}

pub(crate) fn spawn(
	arg: &crate::Arg,
	serve_arg: &crate::serve::Arg,
) -> tg::Result<tokio::process::Child> {
	prepare_sandbox_directory(&arg.path)?;
	let user = prepare_etc_files(&arg.path, arg.network, arg.user.as_deref())?;
	let upper_path = Sandbox::host_upper_path_from_root(&arg.path);
	for mount in &arg.mounts {
		crate::root::ensure_mount_target(&arg.rootfs_path, &upper_path, mount)?;
	}
	let init_arg = super::init::Arg {
		serve: serve_arg.clone(),
	};
	let mut command = tokio::process::Command::new(&arg.tangram_path);
	command.arg("sandbox").arg("container").arg("run");
	command
		.arg("--unshare-all")
		.arg("--as-pid-1")
		.arg("--die-with-parent")
		.arg("--new-session")
		.arg("--uid")
		.arg(user.uid.to_string())
		.arg("--gid")
		.arg(user.gid.to_string())
		.arg("--chdir")
		.arg("/")
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
	if arg.network {
		command.arg("--share-net");
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
		.arg(&arg.artifacts_path)
		.arg(Sandbox::guest_artifacts_path_from_host_artifacts_path(
			&arg.artifacts_path,
		))
		.arg("--ro-bind")
		.arg(&arg.tangram_path)
		.arg(Sandbox::guest_libexec_tangram_path())
		.arg("--bind")
		.arg(Sandbox::host_listen_path_from_root(&arg.path))
		.arg(Sandbox::guest_listen_path_from_root(&arg.path))
		.arg("--bind")
		.arg(Sandbox::host_tangram_socket_path_from_root(&arg.path))
		.arg(Sandbox::guest_tangram_socket_path_from_root(&arg.path))
		.arg("--bind")
		.arg(Sandbox::host_output_path_from_root(&arg.path))
		.arg(Sandbox::guest_output_path_from_root(&arg.path));
	if arg.network && Sandbox::host_resolv_conf_path_from_root(&arg.path).exists() {
		command
			.arg("--ro-bind")
			.arg(Sandbox::host_resolv_conf_path_from_root(&arg.path))
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
		.arg("--cgroup-memory-oom-group")
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
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit());
	command
		.spawn()
		.map_err(|source| tg::error!(!source, "failed to spawn sandbox container"))
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
			|source| tg::error!(!source, path = %path.display(), "failed to create the sandbox path"),
		)?;
	}
	let permissions =
		<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o1777);
	let tmp_path = Sandbox::host_tmp_path_from_root(sandbox_path);
	std::fs::set_permissions(&tmp_path, permissions).map_err(|source| {
		tg::error!(
			!source,
			path = %tmp_path.display(),
			"failed to set sandbox path permissions"
		)
	})?;
	let upper_path = Sandbox::host_upper_path_from_root(sandbox_path);
	let tangram_path = upper_path.join("opt/tangram");
	std::fs::create_dir_all(&tangram_path).map_err(|source| {
		tg::error!(
			!source,
			path = %tangram_path.display(),
			"failed to create the sandbox path"
		)
	})?;
	Ok(())
}

fn prepare_etc_files(sandbox_path: &Path, network: bool, user: Option<&str>) -> tg::Result<User> {
	let user = resolve_user(user)?;
	let passwd = render_passwd(&user);
	std::fs::write(Sandbox::host_passwd_path_from_root(sandbox_path), passwd)
		.map_err(|source| tg::error!(!source, "failed to write /etc/passwd"))?;
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
	.map_err(|source| tg::error!(!source, "failed to write /etc/nsswitch.conf"))?;
	if network {
		std::fs::copy(
			"/etc/resolv.conf",
			Sandbox::host_resolv_conf_path_from_root(sandbox_path),
		)
		.map_err(|source| tg::error!(!source, "failed to stage /etc/resolv.conf"))?;
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
				.map_err(|source| tg::error!(!source, "failed to encode the user name"))?;
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
