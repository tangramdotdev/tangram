use {
	std::{
		ffi::CString,
		os::{
			fd::{AsRawFd as _, FromRawFd as _, RawFd},
			unix::ffi::OsStrExt as _,
		},
		path::{Path, PathBuf},
		sync::Arc,
	},
	tangram_client::prelude::*,
	virtiofsd::{
		passthrough::{self, CachePolicy, InodeFileHandlesMode, PassthroughFs},
		vhost_user::VhostUserFsBackendBuilder,
	},
};

pub struct Bind {
	pub source: PathBuf,
	pub target: PathBuf,
}

// Fork a helper process that serves the shared directory with virtiofsd and return its pid.
pub fn spawn(
	uid: libc::uid_t,
	gid: libc::gid_t,
	shared_dir: &Path,
	socket_path: &Path,
	binds: &[Bind],
	log_path: &Path,
) -> tg::Result<libc::pid_t> {
	let listener = vhost::vhost_user::Listener::new(socket_path, true)
		.map_err(|error| tg::error!(?error, "failed to create the vhost-user listener"))?;
	let listener_fd = listener.as_raw_fd();

	let child = unsafe { libc::fork() };
	if child < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to fork the virtiofsd helper"));
	}
	if child == 0 {
		match child_main(uid, gid, shared_dir, listener_fd, binds, log_path) {
			Ok(()) => std::process::exit(0),
			Err(error) => {
				eprintln!("{error}");
				eprintln!("{}", error.trace());
				std::process::exit(105);
			},
		}
	}

	std::mem::forget(listener);
	unsafe { libc::close(listener_fd) };

	Ok(child)
}

fn child_main(
	uid: libc::uid_t,
	gid: libc::gid_t,
	shared_dir: &Path,
	listener_fd: RawFd,
	binds: &[Bind],
	log_path: &Path,
) -> tg::Result<()> {
	// Enter a user and mount namespace so the bind mounts succeed without privileges and remain
	// private to this helper.
	enter_namespace(uid, gid)?;

	// Bind mount the user mounts into the share dir so virtiofsd serves them to the guest.
	for bind in binds {
		let source = CString::new(bind.source.as_os_str().as_bytes())
			.map_err(|error| tg::error!(!error, "failed to encode the bind source path"))?;
		let target = CString::new(bind.target.as_os_str().as_bytes())
			.map_err(|error| tg::error!(!error, "failed to encode the bind target path"))?;
		let result = unsafe {
			libc::mount(
				source.as_ptr(),
				target.as_ptr(),
				std::ptr::null(),
				libc::MS_BIND | libc::MS_REC,
				std::ptr::null(),
			)
		};
		if result != 0 {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(
				!error,
				source = %bind.source.display(),
				target = %bind.target.display(),
				"failed to bind the user mount",
			));
		}
	}

	// Redirect stdio to the log file.
	let log = std::fs::OpenOptions::new()
		.create(true)
		.append(true)
		.open(log_path)
		.map_err(
			|error| tg::error!(!error, path = %log_path.display(), "failed to open the log file"),
		)?;
	let log_fd = log.as_raw_fd();
	for target in [libc::STDOUT_FILENO, libc::STDERR_FILENO] {
		if unsafe { libc::dup2(log_fd, target) } < 0 {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(
				!error,
				"failed to redirect stdio to the log file"
			));
		}
	}
	drop(log);

	// Enable logging. virtiofsd uses log instead of tracing.
	let _logger = env_logger::Builder::new()
		.filter_level(log::LevelFilter::Warn)
		.target(env_logger::Target::Stderr)
		.try_init();

	// Create the filesystem.
	let config = passthrough::Config {
		root_dir: shared_dir.to_string_lossy().into_owned(),
		cache_policy: CachePolicy::Auto,
		inode_file_handles: InodeFileHandlesMode::Never,
		xattr: true,
		announce_submounts: false,
		clean_noatime: false,
		..passthrough::Config::default()
	};
	let fs = PassthroughFs::new(config)
		.map_err(|error| tg::error!(%error, "failed to construct the passthrough filesystem"))?;

	// Construct the backend.
	let backend = Arc::new(
		VhostUserFsBackendBuilder::default()
			.set_thread_pool_size(0)
			.set_tag(None)
			.build(fs)
			.map_err(|error| tg::error!(%error, "failed to build the vhost-user fs backend"))?,
	);

	// Spawn the daemon.
	let mut daemon = vhost_user_backend::VhostUserDaemon::new(
		String::from("tangram-virtiofsd"),
		backend,
		vm_memory::GuestMemoryAtomic::new(vm_memory::GuestMemoryMmap::new()),
	)
	.map_err(|error| tg::error!(%error, "failed to create the vhost-user daemon"))?;
	let listener = unsafe { vhost::vhost_user::Listener::from_raw_fd(listener_fd) };
	daemon
		.start(listener)
		.map_err(|error| tg::error!(?error, "failed to start the vhost-user daemon"))?;
	if let Err(error) = daemon.wait()
		&& !matches!(
			error,
			vhost_user_backend::Error::HandleRequest(vhost::vhost_user::Error::Disconnected),
		) {
		return Err(tg::error!(
			?error,
			"the vhost-user daemon exited with an error"
		));
	}
	Ok(())
}

fn enter_namespace(uid: libc::uid_t, gid: libc::gid_t) -> tg::Result<()> {
	let host_uid = unsafe { libc::getuid() };
	let host_gid = unsafe { libc::getgid() };
	if unsafe { libc::unshare(libc::CLONE_NEWUSER) } != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to unshare the user namespace"));
	}
	std::fs::write("/proc/self/uid_map", format!("{uid} {host_uid} 1\n"))
		.map_err(|error| tg::error!(!error, "failed to write the uid map"))?;
	std::fs::write("/proc/self/setgroups", "deny")
		.map_err(|error| tg::error!(!error, "failed to deny setgroups"))?;
	std::fs::write("/proc/self/gid_map", format!("{gid} {host_gid} 1\n"))
		.map_err(|error| tg::error!(!error, "failed to write the gid map"))?;
	if unsafe { libc::unshare(libc::CLONE_NEWNS) } != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to unshare the mount namespace"));
	}
	let root = CString::new("/").unwrap();
	let result = unsafe {
		libc::mount(
			std::ptr::null(),
			root.as_ptr(),
			std::ptr::null(),
			libc::MS_REC | libc::MS_PRIVATE,
			std::ptr::null(),
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			"failed to make the mount namespace private"
		));
	}
	Ok(())
}
