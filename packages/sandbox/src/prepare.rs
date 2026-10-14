use {
	crate::{
		linux, mount,
		run::{Backend, Command, Network, Options},
	},
	std::{
		ffi::CString,
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
			unix::ffi::OsStrExt as _,
		},
		path::{Path, PathBuf},
		process::ExitCode,
	},
	tangram_client::prelude::*,
};

pub(crate) struct Output {
	_filesystem: Option<OwnedFd>,
	cgroup: Option<crate::cgroup::Cgroup>,
	cgroup_handle: Option<crate::cgroup::Handle>,
	root: Option<tangram_util::fs::Temp>,
	root_path: Option<PathBuf>,
}

pub(crate) fn prepare(arg: &mut Options, backend: &Backend) -> tg::Result<Output> {
	// Validate the launch configuration.
	if arg.nice > 19 {
		return Err(tg::error!("the CPU priority must be between zero and 19"));
	}
	if arg.rlimit_nofile == Some(0) {
		return Err(tg::error!("the open file limit must be greater than zero"));
	}
	if arg.filesystem_fd.is_some() != arg.filesystem_path.is_some() {
		return Err(tg::error!(
			"the filesystem descriptor and path must be provided together"
		));
	}
	if arg.fuse_fd.is_some() != arg.fuse_path.is_some() {
		return Err(tg::error!(
			"the FUSE descriptor and path must be provided together"
		));
	}

	// Validate the namespace preparation options.
	if matches!(backend, Backend::CloudHypervisor(_))
		&& (arg.filesystem_fd.is_some()
			|| arg.fuse_fd.is_some()
			|| arg.user_namespace_fd.is_some()
			|| arg.isolate_root
			|| arg.cgroup_readonly
			|| arg.network.is_some()
			|| !arg.mounts.binds.is_empty()
			|| !arg.mounts.ro_binds.is_empty()
			|| !arg.mounts.overlays.is_empty()
			|| !arg.mounts.overlay_sources.is_empty()
			|| !arg.mounts.devs.is_empty()
			|| !arg.mounts.procs.is_empty()
			|| !arg.mounts.tmpfs.is_empty()
			|| arg.mounts.filesystem_mount_fd.is_some()
			|| arg.mounts.nodev
			|| arg.mounts.nosuid)
	{
		return Err(tg::error!(
			"cloud-hypervisor does not support namespace preparation options"
		));
	}
	if arg.fuse_fd.is_some() && !matches!(backend, Backend::Builtin(_)) {
		return Err(tg::error!(
			"direct FUSE mounting requires the builtin backend"
		));
	}

	// Configure the parent death signal.
	crate::util::set_parent_death_signal(libc::SIGKILL)?;

	// Create the cgroup.
	let cgroup = arg
		.cgroup
		.as_ref()
		.map(|options| {
			let entry = crate::cgroup::Options {
				cpu: options.cpu,
				memory: options.memory.map(Into::into),
				memory_oom_group: options.memory_oom_group,
				memory_swap: options.memory_swap,
				pids: options.pids,
				..Default::default()
			};
			crate::cgroup::Cgroup::new(&options.name, &entry)
		})
		.transpose()?;
	let cgroup_handle = cgroup
		.as_ref()
		.map(crate::cgroup::Cgroup::handle)
		.transpose()?;

	// Resolve the identity used to prepare the filesystem.
	let (uid, gid) = match backend {
		Backend::Builtin(arg) => (arg.uid, arg.gid),
		Backend::Bubblewrap(arg) => (arg.uid, arg.gid),
		Backend::Gvisor(arg) => (arg.uid, arg.gid),
		Backend::CloudHypervisor(_) => {
			return Ok(Output {
				_filesystem: None,
				cgroup,
				cgroup_handle,
				root: None,
				root_path: None,
			});
		},
	};

	// Enter the user namespace.
	if let Some(fd) = arg.user_namespace_fd {
		linux::enter_user_namespace_with_parent(fd)?;
	} else {
		linux::enter_user_namespace(uid, gid)?;
	}

	// Prepare the host filesystem namespace.
	let host_namespace = if arg.user_namespace_fd.is_some() && arg.filesystem_fd.is_some() {
		Some(crate::filesystem::host_namespace(uid, gid)?)
	} else {
		None
	};

	// Prepare the network and mount namespaces.
	setup_network(arg)?;
	linux::unshare(
		libc::CLONE_NEWNS | libc::CLONE_NEWPID,
		"failed to create the resource setup namespaces",
	)?;

	// Prepare the sandbox filesystem.
	let filesystem = prepare_filesystem(arg, host_namespace.as_ref(), uid, gid)?;
	if let (Some(fd), Some(path)) = (arg.fuse_fd, &arg.fuse_path) {
		crate::builtin::run::mount_fuse(fd, path)?;
	}

	// Create the prepared root.
	let root = if arg.isolate_root
		|| arg
			.mounts
			.binds
			.iter()
			.chain(&arg.mounts.ro_binds)
			.any(|bind| bind.target == Path::new("/"))
		|| arg
			.mounts
			.overlays
			.iter()
			.any(|overlay| overlay.target == Path::new("/"))
	{
		let temp = tangram_util::fs::Temp::new()
			.map_err(|error| tg::error!(!error, "failed to create the prepared root"))?;
		std::fs::create_dir_all(temp.path().join("root"))
			.map_err(|error| tg::error!(!error, "failed to create the prepared root"))?;
		Some(temp)
	} else {
		None
	};
	let root_path = root
		.as_ref()
		.map(|root| root.path().join("root"))
		.or_else(|| match backend {
			Backend::Builtin(arg) => arg.root.clone(),
			Backend::Bubblewrap(arg) => arg.root.clone(),
			Backend::Gvisor(arg) => arg.root.clone(),
			Backend::CloudHypervisor(_) => None,
		});

	let output = Output {
		_filesystem: filesystem,
		cgroup,
		cgroup_handle,
		root,
		root_path,
	};

	Ok(output)
}

pub(crate) fn run(
	command: &Command,
	options: &Options,
	backend: Backend,
	prepared: Output,
) -> tg::Result<ExitCode> {
	// Fork the backend while retaining ownership of its resources.
	let child = linux::fork()?;
	if child == 0 {
		if let Err(error) = child_main(command, options, backend, &prepared) {
			eprintln!("{}", error.trace());
			std::process::exit(105);
		}
		std::process::exit(0);
	}

	// Wait for the backend and detach the root.
	let status = linux::wait_for_child(child);
	if prepared.root.is_some()
		&& let Some(root) = &prepared.root_path
	{
		let root = CString::new(root.as_os_str().as_bytes())
			.map_err(|error| tg::error!(!error, "failed to encode the prepared root"))?;

		// SAFETY: The root is a live, terminated path in the preparation mount namespace.
		if unsafe { libc::umount2(root.as_ptr(), libc::MNT_DETACH) } != 0 {
			let error = std::io::Error::last_os_error();
			if !matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOENT)) {
				return Err(tg::error!(!error, "failed to detach the prepared root"));
			}
		}
	}

	// Clean up the cgroup after the backend exits.
	drop(prepared.cgroup_handle);
	let cleanup = prepared
		.cgroup
		.map(crate::cgroup::Cgroup::cleanup)
		.transpose();
	let status = status?;
	cleanup?;

	Ok(ExitCode::from(status))
}

fn child_main(
	command: &Command,
	arg: &Options,
	backend: Backend,
	prepared: &Output,
) -> tg::Result<()> {
	// Resolve the prepared resources.
	let root = prepared.root_path.as_deref();
	let cgroup = prepared.cgroup_handle.as_ref();

	// Configure the process lifetime and priority.
	crate::util::set_parent_death_signal(libc::SIGKILL)?;

	// SAFETY: nice takes a scalar priority adjustment and errno is thread local.
	unsafe {
		*libc::__errno_location() = 0;
	}

	// SAFETY: nice takes a scalar priority adjustment.
	if unsafe { libc::nice(i32::from(arg.nice)) } == -1
		&& std::io::Error::last_os_error().raw_os_error() != Some(0)
	{
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to lower the sandbox CPU priority"
		));
	}

	// Enter the cgroup and configure the resource limits.
	if let Some(cgroup) = cgroup {
		cgroup.move_self()?;
	}
	if !matches!(backend, Backend::CloudHypervisor(_)) {
		linux::unshare(
			libc::CLONE_NEWCGROUP,
			"failed to create the resource cgroup namespace",
		)?;
	}
	linux::set_rlimit_nofile(arg.rlimit_nofile)?;

	// Run the VM with the common resource settings applied.
	let backend = match backend {
		Backend::CloudHypervisor(arg) => {
			let status = crate::cloud_hypervisor::run::run(command, &arg)?;
			std::process::exit(i32::from(status));
		},
		backend => backend,
	};

	// Prepare the root filesystem.
	if prepared.root.is_some()
		&& let Some(root) = root
		&& !arg
			.mounts
			.binds
			.iter()
			.chain(&arg.mounts.ro_binds)
			.any(|bind| bind.target == Path::new("/"))
		&& !arg
			.mounts
			.overlays
			.iter()
			.any(|overlay| overlay.target == Path::new("/"))
	{
		mount::mount_tmpfs(root)?;
	}
	mount::apply(&arg.mounts, root)?;

	// Mount the cgroup filesystem.
	let cgroup_path = root.map_or_else(
		|| PathBuf::from("/sys/fs/cgroup"),
		|root| root.join("sys/fs/cgroup"),
	);
	mount::mount_cgroup(&cgroup_path, arg.cgroup_readonly)?;

	// Close preparation descriptors before invoking the backend.
	linux::close_non_std_fds()?;

	// Pass only the prepared isolation inputs to the backend.
	match backend {
		Backend::Builtin(mut arg) => {
			if let Some(root) = root {
				arg.root = Some(root.to_owned());
			}
			let status = crate::builtin::run::run(command, &arg)?;
			std::process::exit(i32::from(status));
		},
		Backend::Bubblewrap(mut arg) => {
			if let Some(root) = root {
				arg.root = Some(root.to_owned());
			}
			let status = crate::bubblewrap::run::run(command, &arg)?;
			std::process::exit(i32::from(status));
		},
		Backend::Gvisor(backend_arg) => {
			// Resolve the prepared root and mounts.
			let root = root
				.or(backend_arg.root.as_deref())
				.ok_or_else(|| tg::error!("gvisor requires a prepared root"))?;
			let mut mounts = backend_arg.mounts.clone();
			for (bind, readonly) in arg
				.mounts
				.binds
				.iter()
				.map(|bind| (bind, false))
				.chain(arg.mounts.ro_binds.iter().map(|bind| (bind, true)))
			{
				if bind.target == Path::new("/") {
					continue;
				}
				let target = bind
					.target
					.strip_prefix("/")
					.map_err(|error| tg::error!(!error, "invalid gvisor mount target"))?;
				let mount = tg::sandbox::Mount {
					readonly,
					source: root.join(target),
					target: bind.target.clone(),
				};
				mounts.push(mount);
			}

			// Describe the prepared execution inputs.
			let network = match arg.network {
				None => None,
				Some(Network::Host) => Some(crate::gvisor::run::Network::Host),
				Some(Network::Pasta { .. } | Network::Veth { .. }) => {
					Some(crate::gvisor::run::Network::Sandbox)
				},
			};
			let prepared = crate::gvisor::run::Prepared {
				mounts,
				network,
				rlimit_nofile: arg.rlimit_nofile,
				root: root.to_owned(),
			};

			// Invoke the backend with its configuration and prepared inputs.
			let status = crate::gvisor::run::run(command, &backend_arg, &prepared)?;
			std::process::exit(i32::from(status));
		},
		Backend::CloudHypervisor(_) => unreachable!(),
	}
}

fn setup_network(arg: &Options) -> tg::Result<()> {
	match &arg.network {
		Some(Network::Host) => {},
		Some(Network::Pasta { fd } | Network::Veth { fd, .. }) => {
			linux::unshare(
				libc::CLONE_NEWNET,
				"failed to create the resource network namespace",
			)?;
			if *fd < 0 {
				return Err(tg::error!(
					"the network requires a valid synchronization descriptor"
				));
			}

			// SAFETY: The inherited synchronization descriptor is exclusively owned here.
			let fd = unsafe { OwnedFd::from_raw_fd(*fd) };
			use std::io::{Read as _, Write as _};
			let mut socket = std::os::unix::net::UnixStream::from(fd);
			socket
				.write_all(&[0])
				.map_err(|error| tg::error!(!error, "failed to signal network readiness"))?;
			socket
				.read_exact(&mut [0])
				.map_err(|error| tg::error!(!error, "failed to wait for network setup"))?;

			if let Some(Network::Veth { gateway, guest, .. }) = &arg.network {
				let suffix = format!("{:09}", arg.index % 1_000_000_000);
				let name = format!("tg-vc-{suffix}");
				let mut nl = crate::netlink::Netlink::new()?;
				nl.link_rename(&name, "eth0")?;
				crate::network::host::disable_ipv6("eth0")?;
				nl.addr_add_v4("eth0", *guest, 16)?;
				nl.link_set_up("eth0")?;
				nl.link_set_up("lo")?;
				nl.route_add_default_v4(*gateway)?;
			}
		},
		None => linux::unshare(
			libc::CLONE_NEWNET,
			"failed to create the resource network namespace",
		)?,
	}

	Ok(())
}

fn prepare_filesystem(
	arg: &mut Options,
	host_namespace: Option<&OwnedFd>,
	uid: u32,
	gid: u32,
) -> tg::Result<Option<OwnedFd>> {
	// Open and prepare the sandbox filesystem.
	let (Some(path), Some(socket)) = (arg.filesystem_path.clone(), arg.filesystem_fd) else {
		return Ok(None);
	};
	let filesystem = crate::filesystem::open(&path)?;
	crate::filesystem::prepare(&filesystem, uid, gid)?;

	// Rewrite the mounts to use the prepared filesystem.
	arg.mounts.filesystem_mount_fd = Some(filesystem.as_raw_fd());
	let target = crate::filesystem::path(&filesystem);
	for bind in arg.mounts.binds.iter_mut().chain(&mut arg.mounts.ro_binds) {
		crate::filesystem::relocate(&mut bind.source, &path, &target);
	}
	for overlay in &mut arg.mounts.overlays {
		crate::filesystem::relocate(&mut overlay.upperdir, &path, &target);
		crate::filesystem::relocate(&mut overlay.workdir, &path, &target);
	}

	// Send the filesystem descriptor to the parent.
	// SAFETY: The inherited filesystem control descriptor is exclusively owned here.
	let socket = unsafe { OwnedFd::from_raw_fd(socket) };
	let host_filesystem = host_namespace
		.map(|namespace| crate::filesystem::host_mount(&filesystem, namespace))
		.transpose()?;
	crate::filesystem::send(&socket, host_filesystem.as_ref().unwrap_or(&filesystem))?;

	Ok(Some(filesystem))
}
