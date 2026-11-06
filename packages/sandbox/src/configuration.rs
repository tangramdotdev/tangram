use {
	crate::{Sandbox, configure},
	std::{
		fmt::Write as _,
		io::{Read as _, Write as _},
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
			unix::net::UnixStream,
		},
		path::{Path, PathBuf},
		sync::{Arc, atomic::Ordering},
	},
	tangram_client::prelude::*,
};

pub(crate) struct Namespaces {
	pub(crate) files: [OwnedFd; 5],
	pub(crate) gid: u32,
	pub(crate) mounts: Vec<(PathBuf, bool)>,
	pub(crate) nested: bool,
	pub(crate) network_user: OwnedFd,
	pub(crate) pid: u32,
	pub(crate) uid: u32,
}

impl Sandbox {
	/// Configure an exclusively owned warm sandbox before assigning it to a workload.
	pub async fn configure(&mut self, request: tg::sandbox::create::Arg) -> tg::Result<()> {
		let state = Arc::get_mut(&mut self.0)
			.ok_or_else(|| tg::error!("the warm sandbox is already shared"))?;
		if state.activated.load(Ordering::Relaxed) || state.stopped.load(Ordering::SeqCst) {
			return Err(tg::error!(
				"the sandbox cannot be configured after activation or stop"
			));
		}

		// Reject targets that duplicate the mandatory startup mounts.
		if let Some(namespaces) = state.namespaces.get_mut().unwrap().as_ref()
			&& request.mounts.iter().any(|mount| {
				namespaces
					.mounts
					.iter()
					.any(|(target, _)| target == &mount.target)
			}) {
			return Err(tg::error!("duplicate mount targets are not supported"));
		}

		// Validate the requested resources and mounts.
		let mut arg = state.arg.clone();
		arg.cpu = request.cpu;
		arg.hostname = request.hostname;
		arg.memory = request.memory;
		arg.mounts = request.mounts;
		arg.mounts.extend(state.arg.mounts.iter().cloned());
		arg.network = request.network.map(|network| match network {
			tg::sandbox::Network::Bridge(bridge) => crate::Network::Bridge(crate::Bridge {
				ports: bridge.ports,
			}),
			tg::sandbox::Network::Default => crate::Network::Default,
			tg::sandbox::Network::Host => crate::Network::Host,
		});
		arg.storage = request.storage;
		if let Some(max) = arg.storage.and_then(|storage| storage.max) {
			arg.resources.max_filesystem_size = Some(max);
		}
		crate::validate(&arg)?;

		// Apply the resource bounds without changing the sandbox's share identity.
		if arg.cpu != state.arg.cpu || arg.memory != state.arg.memory {
			let cgroup = state.cgroup.get_mut().take();
			let updated = arg.clone();
			let previous_cpu = state.arg.cpu;
			let previous_memory = state.arg.memory;
			let cpu_changed = arg.cpu != state.arg.cpu;
			let memory_changed = arg.memory != state.arg.memory;
			let (cgroup, result) = tokio::task::spawn_blocking(move || {
				let mut cgroup = cgroup;
				let result = (|| {
					let cgroup = cgroup
						.as_mut()
						.ok_or_else(|| tg::error!("the warm sandbox has no resource cgroup"))?;
					if cpu_changed {
						cgroup.set_cpu(&updated, previous_cpu)?;
					}
					if memory_changed {
						cgroup.set_memory(updated.memory, previous_memory)?;
					}
					Ok::<_, tg::Error>(())
				})();
				(cgroup, result)
			})
			.await
			.map_err(|error| tg::error!(!error, "the resource configuration task panicked"))?;
			*state.cgroup.get_mut() = cgroup;
			result?;
		}
		if arg.storage.unwrap_or_default() != state.arg.storage.unwrap_or_default() {
			let storage = state
				.storage
				.get_mut()
				.unwrap()
				.as_ref()
				.ok_or_else(|| tg::error!("the warm sandbox has no storage allocation"))?;
			storage.set_resource(arg.storage.unwrap_or_default())?;
		}

		// Configure the isolated network and retain its resources before running a helper.
		let mut network_arg = None;
		if let Some(network) = &arg.network {
			if matches!(network, crate::Network::Host) {
				return Err(tg::error!(
					"a private network namespace cannot become host networking"
				));
			}
			let namespaces = state
				.namespaces
				.get_mut()
				.unwrap()
				.as_ref()
				.ok_or_else(|| tg::error!("the sandbox has no prepared namespaces"))?;
			let mut network = crate::create_network(&arg)?.unwrap();
			match &mut network {
				crate::network::Network::Veth(network) => {
					let index = arg.index;
					let bridge = network.bridge_name().to_owned();
					let mut pair = tokio::task::spawn_blocking(move || {
						crate::network::veth::Pair::new(index, &bridge)
					})
					.await
					.map_err(|error| tg::error!(!error, "the network creation task panicked"))??;
					pair.attach(namespaces.pid)?;
					network_arg = Some(configure::Network {
						gateway: network.gateway_ip(),
						guest: network.guest_ip(),
						name: pair.guest_name.clone(),
					});
					let mut contents = String::new();
					for dns in network.dns() {
						writeln!(contents, "nameserver {dns}").unwrap();
					}
					std::fs::write(
						Sandbox::host_resolv_conf_path_from_root(&arg.path),
						contents,
					)
					.map_err(|error| {
						tg::error!(!error, "failed to configure the sandbox resolver")
					})?;
				},
				crate::network::Network::Pasta(network) => {
					let cgroup = state
						.cgroup
						.get_mut()
						.as_ref()
						.map(|cgroup| {
							if arg.resources.cgroup_readonly {
								cgroup.handle()
							} else {
								cgroup.child(crate::cgroup::SIDECARS)
							}
						})
						.transpose()?;
					let directory = cgroup.map(|cgroup| cgroup.open_fd()).transpose()?;
					let namespace = namespaces.files[1].try_clone().map_err(|error| {
						tg::error!(!error, "failed to clone the network namespace")
					})?;
					let user = namespaces.network_user.try_clone().map_err(|error| {
						tg::error!(!error, "failed to clone the user namespace")
					})?;
					let mut holder =
						configure::hold_command(&arg.tangram_path, &namespace, &user).await?;
					let pid = holder.id().ok_or_else(|| {
						tg::error!("the namespace holder exited before configuration")
					})?;
					let result = network
						.start_namespace(pid, namespace, user, |command| {
							if let Some(directory) = &directory {
								crate::cgroup::enter_before_exec(command, directory.as_raw_fd());
							}
						})
						.await;
					drop(holder.stdin.take());
					let status = holder.wait().await.map_err(|error| {
						tg::error!(!error, "failed to wait for the namespace holder")
					})?;
					result?;
					if !status.success() {
						return Err(tg::error!(%status, "the namespace holder failed"));
					}
					let contents =
						format!("nameserver {}\n", crate::network::pasta::DNS_FORWARD_IP);
					std::fs::write(
						Sandbox::host_resolv_conf_path_from_root(&arg.path),
						contents,
					)
					.map_err(|error| {
						tg::error!(!error, "failed to configure the sandbox resolver")
					})?;
				},
				crate::network::Network::Host
				| crate::network::Network::Passt(_)
				| crate::network::Network::Tap(_) => unreachable!(),
			}
			*state.network.get_mut().unwrap() = Some(network);
		}

		// Apply the hostname, mounts, and interface configuration in one helper invocation.
		let hostname = (arg.hostname != state.arg.hostname)
			.then(|| arg.hostname.clone())
			.flatten();
		let mounts = &arg.mounts[..arg.mounts.len() - state.arg.mounts.len()];
		if hostname.is_some() || !mounts.is_empty() || network_arg.is_some() {
			let namespaces = state
				.namespaces
				.get_mut()
				.unwrap()
				.as_ref()
				.ok_or_else(|| tg::error!("the sandbox has no prepared namespaces"))?;
			let mut fds = [0; 6];
			fds[..5].copy_from_slice(&namespaces.files.each_ref().map(OwnedFd::as_raw_fd));
			fds[5] = namespaces.network_user.as_raw_fd();
			let mut input = configure::Arg {
				fds,
				gid: namespaces.gid,
				hostname,
				mounts: mounts.to_vec(),
				network: network_arg,
				pid: namespaces.nested.then_some(namespaces.pid),
				restore: namespaces
					.mounts
					.iter()
					.filter(|(target, _)| {
						mounts.iter().any(|mount| {
							target != &mount.target && target.starts_with(&mount.target)
						})
					})
					.cloned()
					.collect(),
				uid: namespaces.uid,
			};
			// Configure distinct namespace owners in separate helpers because either owner can be an ancestor of the other.
			if input.network.is_some() {
				let mount_user = rustix::fs::fstat(&namespaces.files[3]).map_err(|error| {
					tg::error!(!error, "failed to inspect the mount namespace owner")
				})?;
				let network_user =
					rustix::fs::fstat(&namespaces.network_user).map_err(|error| {
						tg::error!(!error, "failed to inspect the network namespace owner")
					})?;
				if mount_user.st_ino != network_user.st_ino {
					let network = configure::Arg {
						fds,
						gid: namespaces.gid,
						hostname: None,
						mounts: Vec::new(),
						network: input.network.take(),
						pid: input.pid,
						restore: Vec::new(),
						uid: namespaces.uid,
					};
					configure::command(&arg.tangram_path, &network).await?;
				}
			}
			if input.hostname.is_some() || !input.mounts.is_empty() || input.network.is_some() {
				configure::command(&arg.tangram_path, &input).await?;
			}
		}
		drop(state.namespaces.get_mut().unwrap().take());
		state.arg = arg;
		Ok(())
	}
}

impl Namespaces {
	pub(crate) fn send(path: &Path) -> tg::Result<()> {
		let mut socket = UnixStream::connect(path)
			.map_err(|error| tg::error!(!error, "failed to connect the namespace socket"))?;
		// Send the workload identity as seen inside its user namespace.
		// SAFETY: getuid and getgid have no preconditions.
		let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
		socket
			.write_all(&uid.to_ne_bytes())
			.and_then(|()| socket.write_all(&gid.to_ne_bytes()))
			.map_err(|error| tg::error!(!error, "failed to send the sandbox identity"))?;
		let socket = OwnedFd::from(socket);
		for path in [
			"/proc/self/ns/mnt",
			"/proc/self/ns/net",
			"/",
			"/proc/self/ns/user",
			"/proc/self/ns/uts",
		] {
			let file = std::fs::File::open(path)
				.map_err(|error| tg::error!(!error, "failed to open a sandbox namespace"))?;
			crate::filesystem::send(&socket, &file.into())?;
		}
		Ok(())
	}

	pub(crate) fn receive(
		mut socket: UnixStream,
		launcher: u32,
		mounts: Vec<(PathBuf, bool)>,
	) -> tg::Result<Self> {
		socket
			.set_nonblocking(false)
			.map_err(|error| tg::error!(!error, "failed to configure the namespace socket"))?;
		let mut credentials = std::mem::MaybeUninit::<libc::ucred>::uninit();
		let mut length = std::mem::size_of_val(&credentials).try_into().unwrap();
		// SAFETY: The socket descriptor and credential output buffers are valid.
		if unsafe {
			libc::getsockopt(
				socket.as_raw_fd(),
				libc::SOL_SOCKET,
				libc::SO_PEERCRED,
				credentials.as_mut_ptr().cast(),
				&raw mut length,
			)
		} != 0
		{
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to identify the namespace sender"
			));
		}
		// SAFETY: The successful getsockopt initialized the credentials.
		let pid = unsafe { credentials.assume_init() }.pid;
		let pid = u32::try_from(pid)
			.map_err(|error| tg::error!(!error, "the namespace sender PID is invalid"))?;
		let mut ancestor = pid;
		while ancestor != launcher {
			if ancestor <= 1 {
				return Err(tg::error!(
					"the namespace sender is not a sandbox descendant"
				));
			}
			let status =
				std::fs::read_to_string(format!("/proc/{ancestor}/status")).map_err(|error| {
					tg::error!(!error, "failed to identify the namespace sender's parent")
				})?;
			ancestor = status
				.lines()
				.find_map(|line| line.strip_prefix("PPid:"))
				.and_then(|pid| pid.trim().parse().ok())
				.ok_or_else(|| tg::error!("missing the namespace sender's parent PID"))?;
		}
		let mut identity = [0_u8; 8];
		socket
			.read_exact(&mut identity)
			.map_err(|error| tg::error!(!error, "failed to receive the sandbox identity"))?;
		let uid = u32::from_ne_bytes(identity[..4].try_into().unwrap());
		let gid = u32::from_ne_bytes(identity[4..].try_into().unwrap());
		let socket = OwnedFd::from(socket);
		let mut files = Vec::new();
		for _ in 0..5 {
			files.push(crate::filesystem::receive(&socket)?);
		}
		let mut files: [OwnedFd; 5] = files.try_into().unwrap();
		// Enter the mount namespace owner because runtimes can add an inner user namespace after creating their mounts and hostname.
		let owner = user_namespace(&files[0])?;
		let user = rustix::fs::fstat(&files[3])
			.map_err(|error| tg::error!(!error, "failed to inspect the runtime user namespace"))?;
		let owner_stat = rustix::fs::fstat(&owner)
			.map_err(|error| tg::error!(!error, "failed to inspect the mount namespace owner"))?;
		let nested = user.st_ino != owner_stat.st_ino;
		files[3] = owner;
		let network_user = user_namespace(&files[1])?;
		Ok(Self {
			files,
			gid,
			mounts,
			nested,
			network_user,
			pid,
			uid,
		})
	}
}

fn user_namespace(namespace: &OwnedFd) -> tg::Result<OwnedFd> {
	const NS_GET_USERNS: libc::c_ulong = 0xb701;
	// SAFETY: The ioctl reads the namespace descriptor and returns a newly owned descriptor.
	let fd = unsafe { libc::ioctl(namespace.as_raw_fd(), NS_GET_USERNS) };
	if fd < 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to open the namespace's owning user namespace"
		));
	}
	// SAFETY: The successful ioctl returned a new descriptor that nothing else owns.
	let fd = unsafe { OwnedFd::from_raw_fd(fd) };
	rustix::io::fcntl_setfd(&fd, rustix::io::FdFlags::CLOEXEC)
		.map_err(|error| tg::error!(!error, "failed to protect the namespace descriptor"))?;
	Ok(fd)
}
