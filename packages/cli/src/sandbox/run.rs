use {
	crate::Cli,
	std::{ffi::OsString, io::Read as _, os::fd::FromRawFd as _, path::PathBuf},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long, conflicts_with_all = ["config_fd", "launch"])]
	pub config: Option<PathBuf>,

	#[arg(long, conflicts_with = "launch")]
	pub config_fd: Option<i32>,

	#[command(flatten)]
	pub options: Options,
}

#[derive(Clone, Debug, clap::Args)]
#[group(id = "launch", multiple = true)]
pub struct Options {
	#[arg(long)]
	pub as_pid_1: bool,

	#[arg(long, value_parser = crate::sandbox::parse_backend)]
	pub backend: Option<tangram_sandbox::Kind>,

	#[arg(long = "bind", action = clap::ArgAction::Append, num_args = 2)]
	pub binds: Vec<PathBuf>,

	#[arg(long = "cap-drop", action = clap::ArgAction::Append)]
	pub cap_drop: Vec<String>,

	#[arg(long)]
	pub cgroup: Option<String>,

	#[arg(long)]
	pub cgroup_cpu: Option<u64>,

	#[arg(long)]
	pub cgroup_memory: Option<u64>,

	#[arg(long)]
	pub cgroup_memory_oom_group: bool,

	#[arg(long)]
	pub cgroup_memory_swap: Option<u64>,

	#[arg(long)]
	pub cgroup_pids: Option<u64>,

	#[arg(long)]
	pub cgroup_readonly: bool,

	#[arg(long)]
	pub chdir: Option<PathBuf>,

	#[arg(long)]
	pub clearenv: bool,

	#[arg(long)]
	pub cloud_hypervisor_path: Option<PathBuf>,

	#[arg(trailing_var_arg = true)]
	pub command: Vec<OsString>,

	#[arg(long)]
	pub cpu: Option<u64>,

	#[arg(long)]
	pub create_snapshot: Option<PathBuf>,

	#[arg(long)]
	pub dax: Option<u64>,

	#[arg(long = "dev", action = clap::ArgAction::Append, num_args = 1)]
	pub devs: Vec<PathBuf>,

	#[arg(long, action = clap::ArgAction::Append)]
	pub dns: Vec<std::net::Ipv4Addr>,

	#[arg(long)]
	pub filesystem_fd: Option<i32>,

	#[arg(long)]
	pub filesystem_path: Option<PathBuf>,

	#[arg(long)]
	pub firewall: Option<tangram_sandbox::Firewall>,

	#[arg(long)]
	pub fuse_fd: Option<i32>,

	#[arg(long)]
	pub fuse_path: Option<PathBuf>,

	#[arg(long)]
	pub gateway_ip: Option<std::net::Ipv4Addr>,

	#[arg(long)]
	pub gid: Option<u32>,

	#[arg(long)]
	pub guest_ip: Option<std::net::Ipv4Addr>,

	#[arg(long)]
	pub host_ip: Option<std::net::Ipv4Addr>,

	#[arg(long)]
	pub hostname: Option<String>,

	#[arg(long)]
	pub http2_connection_window_size: Option<u32>,

	#[arg(long)]
	pub http2_max_concurrent_reset_streams: Option<usize>,

	#[arg(long)]
	pub http2_max_concurrent_streams: Option<u32>,

	#[arg(long)]
	pub http2_stream_window_size: Option<u32>,

	#[arg(long)]
	pub image_path: Option<PathBuf>,

	#[arg(long)]
	pub index: Option<u64>,

	#[arg(long)]
	pub kernel_path: Option<PathBuf>,

	#[arg(long)]
	pub max_cpu: Option<u64>,

	#[arg(long)]
	pub max_memory: Option<u64>,

	#[arg(long)]
	pub memory: Option<u64>,

	#[arg(long = "mount", action = clap::ArgAction::Append)]
	pub mounts: Vec<tg::sandbox::Mount>,

	#[arg(long)]
	pub network: Option<String>,

	#[arg(long)]
	pub network_fd: Option<i32>,

	#[arg(long, value_parser = humantime::parse_duration)]
	pub network_interface_timeout: Option<std::time::Duration>,

	#[arg(long, value_parser = humantime::parse_duration)]
	pub network_startup_timeout: Option<std::time::Duration>,

	#[arg(long)]
	pub nice: Option<u8>,

	#[arg(long = "overlay-src", action = clap::ArgAction::Append, num_args = 1)]
	pub overlay_sources: Vec<PathBuf>,

	#[arg(long = "overlay", action = clap::ArgAction::Append, num_args = 3)]
	pub overlays: Vec<PathBuf>,

	#[arg(long)]
	pub path: Option<PathBuf>,

	#[arg(long, value_parser = parse_platform)]
	pub platform: Option<tangram_sandbox::Platform>,

	#[arg(long = "port", action = clap::ArgAction::Append)]
	pub ports: Vec<tg::sandbox::Port>,

	#[arg(long = "proc", action = clap::ArgAction::Append, num_args = 1)]
	pub procs: Vec<PathBuf>,

	#[arg(long)]
	pub profile: Option<PathBuf>,

	#[arg(long, value_parser = humantime::parse_duration)]
	pub ready_timeout: Option<std::time::Duration>,

	#[arg(long)]
	pub rlimit_nofile: Option<u64>,

	#[arg(long = "ro-bind", action = clap::ArgAction::Append, num_args = 2)]
	pub ro_binds: Vec<PathBuf>,

	#[arg(long)]
	pub rootfs_path: Option<PathBuf>,

	#[arg(long)]
	pub runtime_path: Option<PathBuf>,

	#[arg(long, value_parser = parse_seccomp)]
	pub seccomp: Option<tangram_sandbox::SeccompPolicy>,

	#[arg(long, value_parser = humantime::parse_duration)]
	pub serial_connect_interval: Option<std::time::Duration>,

	#[arg(long, value_parser = humantime::parse_duration)]
	pub serial_connect_timeout: Option<std::time::Duration>,

	#[arg(long = "setenv", action = clap::ArgAction::Append, num_args = 2)]
	pub setenvs: Vec<String>,

	#[arg(long)]
	pub snapshot: Option<PathBuf>,

	#[arg(long)]
	pub snapshot_cpu: Option<u64>,

	#[arg(long)]
	pub snapshot_memory: Option<u64>,

	#[arg(long)]
	pub store_path: Option<PathBuf>,

	#[arg(long)]
	pub tangram_path: Option<PathBuf>,

	#[arg(long = "tmpfs", action = clap::ArgAction::Append, num_args = 1)]
	pub tmpfs: Vec<PathBuf>,

	#[arg(long)]
	pub uid: Option<u32>,

	#[arg(long)]
	pub unshare_all: bool,

	#[arg(long)]
	pub url: Option<tangram_uri::Uri>,

	#[arg(long)]
	pub user_namespace_fd: Option<i32>,
}

impl Cli {
	pub fn command_sandbox_run(args: Args) -> tg::Result<std::process::ExitCode> {
		let arg = if let Some(path) = args.config {
			let contents = std::fs::read(&path).map_err(|error| tg::error!(!error, path = %path.display(), "failed to read the sandbox launch configuration"))?;
			read_config(&contents)?
		} else if let Some(fd) = args.config_fd {
			if fd < 3 {
				return Err(tg::error!(
					"the configuration descriptor must be greater than two"
				));
			}

			// SAFETY: Duplicating the numeric descriptor validates it before taking ownership.
			let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
			if duplicate < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"invalid sandbox configuration descriptor"
				));
			}

			// SAFETY: A successful duplication returns a newly owned descriptor.
			let mut file = unsafe { std::fs::File::from_raw_fd(duplicate) };

			// SAFETY: The original descriptor was successfully validated and is transferred to this command.
			drop(unsafe { std::fs::File::from_raw_fd(fd) });
			let mut contents = Vec::new();
			file.read_to_end(&mut contents).map_err(|error| {
				tg::error!(
					!error,
					"failed to read the sandbox configuration descriptor"
				)
			})?;

			read_config(&contents)?
		} else {
			args.options.arg()?
		};

		tangram_sandbox::run::run(arg)
	}
}

impl Options {
	fn arg(self) -> tg::Result<tangram_sandbox::run::Arg> {
		let backend = self.backend.unwrap_or({
			if cfg!(target_os = "macos") {
				tangram_sandbox::Kind::Seatbelt
			} else {
				tangram_sandbox::Kind::Builtin
			}
		});
		self.validate(backend)?;
		let arg = match backend {
			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::Builtin
			| tangram_sandbox::Kind::Bubblewrap
			| tangram_sandbox::Kind::Gvisor => self.linux(backend)?,

			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::CloudHypervisor => self.cloud_hypervisor()?,

			#[cfg(target_os = "macos")]
			tangram_sandbox::Kind::Seatbelt => {
				let command = self.command()?;
				let backend = tangram_sandbox::seatbelt::run::Arg {
					profile: self
						.profile
						.ok_or_else(|| tg::error!("the seatbelt profile is required"))?,
				};
				tangram_sandbox::run::Arg {
					backend: tangram_sandbox::run::Backend::Seatbelt(backend),
					command,
					options: tangram_sandbox::run::Options::default(),
				}
			},

			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::Seatbelt => {
				return Err(tg::error!("the seatbelt backend requires macos"));
			},

			#[cfg(target_os = "macos")]
			_ => return Err(tg::error!("the selected backend requires linux")),
		};

		Ok(arg)
	}

	fn validate(&self, backend: tangram_sandbox::Kind) -> tg::Result<()> {
		use tangram_sandbox::Kind;

		// Validate the Linux launch options.
		let linux = matches!(backend, Kind::Builtin | Kind::Bubblewrap | Kind::Gvisor);
		let linux_options = self.as_pid_1
			|| !self.binds.is_empty()
			|| !self.cap_drop.is_empty()
			|| self.cgroup_readonly
			|| !self.devs.is_empty()
			|| self.filesystem_fd.is_some()
			|| self.filesystem_path.is_some()
			|| self.fuse_fd.is_some()
			|| self.fuse_path.is_some()
			|| self.gid.is_some()
			|| self.network_fd.is_some()
			|| !self.overlay_sources.is_empty()
			|| !self.overlays.is_empty()
			|| !self.procs.is_empty()
			|| !self.ro_binds.is_empty()
			|| !self.tmpfs.is_empty()
			|| self.uid.is_some()
			|| self.unshare_all
			|| self.user_namespace_fd.is_some();
		if linux_options && !linux {
			return Err(tg::error!(
				"the selected backend does not support Linux launch options"
			));
		}

		// Validate the cloud-hypervisor launch options.
		let cloud_options = self.cloud_hypervisor_path.is_some()
			|| self.cpu.is_some()
			|| self.create_snapshot.is_some()
			|| self.dax.is_some()
			|| !self.dns.is_empty()
			|| self.firewall.is_some()
			|| self.host_ip.is_some()
			|| self.http2_connection_window_size.is_some()
			|| self.http2_max_concurrent_reset_streams.is_some()
			|| self.http2_max_concurrent_streams.is_some()
			|| self.http2_stream_window_size.is_some()
			|| self.image_path.is_some()
			|| self.kernel_path.is_some()
			|| self.max_cpu.is_some()
			|| self.max_memory.is_some()
			|| self.memory.is_some()
			|| !self.mounts.is_empty()
			|| self.network_interface_timeout.is_some()
			|| self.network_startup_timeout.is_some()
			|| self.path.is_some()
			|| !self.ports.is_empty()
			|| self.ready_timeout.is_some()
			|| self.rootfs_path.is_some()
			|| self.serial_connect_interval.is_some()
			|| self.serial_connect_timeout.is_some()
			|| self.snapshot.is_some()
			|| self.snapshot_cpu.is_some()
			|| self.snapshot_memory.is_some()
			|| self.store_path.is_some()
			|| self.tangram_path.is_some()
			|| self.url.is_some();
		if cloud_options && backend != Kind::CloudHypervisor {
			return Err(tg::error!(
				"the selected backend does not support cloud-hypervisor launch options"
			));
		}

		// Validate the backend-specific options.
		if self.platform.is_some() && backend != Kind::Gvisor {
			return Err(tg::error!(
				"the platform option requires the gvisor backend"
			));
		}
		if self.runtime_path.is_some() && !matches!(backend, Kind::Bubblewrap | Kind::Gvisor) {
			return Err(tg::error!(
				"the runtime path requires the bubblewrap or gvisor backend"
			));
		}
		if self.as_pid_1 && !matches!(backend, Kind::Builtin | Kind::Bubblewrap) {
			return Err(tg::error!(
				"the PID 1 option requires the builtin or bubblewrap backend"
			));
		}

		if self.seccomp.is_some() && !matches!(backend, Kind::Builtin | Kind::Bubblewrap) {
			return Err(tg::error!(
				"a host seccomp policy requires the builtin or bubblewrap backend"
			));
		}
		if self.profile.is_some() && backend != Kind::Seatbelt {
			return Err(tg::error!(
				"the profile option requires the seatbelt backend"
			));
		}
		if backend == Kind::CloudHypervisor
			&& !self.command.is_empty()
			&& (self.url.is_some()
				|| self.http2_connection_window_size.is_some()
				|| self.http2_max_concurrent_reset_streams.is_some()
				|| self.http2_max_concurrent_streams.is_some()
				|| self.http2_stream_window_size.is_some())
		{
			return Err(tg::error!(
				"an explicit guest command cannot be combined with sandbox server options"
			));
		}

		// Validate the common resource and network options.
		if !cfg!(target_os = "linux")
			&& (self.cgroup.is_some()
				|| self.cgroup_cpu.is_some()
				|| self.cgroup_memory.is_some()
				|| self.cgroup_memory_oom_group
				|| self.cgroup_memory_swap.is_some()
				|| self.cgroup_pids.is_some()
				|| self.nice.is_some()
				|| self.rlimit_nofile.is_some())
		{
			return Err(tg::error!("the resource options require linux"));
		}
		if self.cgroup.is_none()
			&& (self.cgroup_cpu.is_some()
				|| self.cgroup_memory.is_some()
				|| self.cgroup_memory_oom_group
				|| self.cgroup_memory_swap.is_some()
				|| self.cgroup_pids.is_some())
		{
			return Err(tg::error!("cgroup limits require a cgroup name"));
		}
		if self.gateway_ip.is_some() && !linux {
			return Err(tg::error!(
				"the gateway address requires a Linux launch backend"
			));
		}
		if backend == Kind::Seatbelt
			&& (self.guest_ip.is_some()
				|| self.hostname.is_some()
				|| self.index.is_some()
				|| self.network.is_some())
		{
			return Err(tg::error!(
				"the selected backend does not support Linux launch options"
			));
		}

		Ok(())
	}

	#[cfg(target_os = "linux")]
	fn linux(self, backend: tangram_sandbox::Kind) -> tg::Result<tangram_sandbox::run::Arg> {
		use tangram_sandbox::run::{Bind, Cgroup, Mounts, Network, Options, Overlay};

		// Validate the command.
		if self.command.is_empty() {
			return Err(tg::error!("a command is required"));
		}
		if self.cap_drop.iter().any(|capability| capability != "ALL") {
			return Err(tg::error!("the launch supports --cap-drop ALL"));
		}

		// Create the command description.
		let command = self.command()?;

		// Create the mount configuration.
		let binds = self
			.binds
			.as_chunks::<2>()
			.0
			.iter()
			.map(|pair| Bind {
				devices: false,
				fd: None,
				source: pair[0].clone(),
				target: pair[1].clone(),
			})
			.chain(
				self.mounts
					.iter()
					.filter(|mount| !mount.readonly)
					.map(|mount| Bind {
						devices: false,
						fd: None,
						source: mount.source.clone(),
						target: mount.target.clone(),
					}),
			)
			.collect();
		let ro_binds = self
			.ro_binds
			.as_chunks::<2>()
			.0
			.iter()
			.map(|pair| Bind {
				devices: false,
				fd: None,
				source: pair[0].clone(),
				target: pair[1].clone(),
			})
			.chain(
				self.mounts
					.iter()
					.filter(|mount| mount.readonly)
					.map(|mount| Bind {
						devices: false,
						fd: None,
						source: mount.source.clone(),
						target: mount.target.clone(),
					}),
			)
			.collect();
		let overlays = self
			.overlays
			.as_chunks::<3>()
			.0
			.iter()
			.map(|triple| Overlay {
				target: triple[2].clone(),
				upperdir: triple[0].clone(),
				workdir: triple[1].clone(),
			})
			.collect();
		let mounts = Mounts {
			binds,
			devs: self.devs,
			nodev: true,
			nosuid: true,
			overlay_sources: self.overlay_sources,
			overlays,
			procs: self.procs,
			ro_binds,
			tmpfs: self.tmpfs,
			..Default::default()
		};

		// Create the network configuration.
		let network = match self.network.as_deref() {
			None | Some("none") => None,
			Some("host") => Some(Network::Host),
			Some("pasta") => Some(Network::Pasta {
				fd: self
					.network_fd
					.ok_or_else(|| tg::error!("the network descriptor is required"))?,
			}),
			Some("veth") => Some(Network::Veth {
				fd: self
					.network_fd
					.ok_or_else(|| tg::error!("the network descriptor is required"))?,
				gateway: self
					.gateway_ip
					.ok_or_else(|| tg::error!("the gateway address is required"))?,
				guest: self
					.guest_ip
					.ok_or_else(|| tg::error!("the guest address is required"))?,
			}),
			Some(kind) => return Err(tg::error!(%kind, "invalid Linux launch network")),
		};

		// Create the resource configuration.
		let cgroup = self.cgroup.map(|name| Cgroup {
			cpu: self.cgroup_cpu,
			memory: self.cgroup_memory,
			memory_oom_group: self.cgroup_memory_oom_group,
			memory_swap: self.cgroup_memory_swap,
			name,
			pids: self.cgroup_pids,
		});
		let options = Options {
			cgroup,
			cgroup_readonly: self.cgroup_readonly,
			filesystem_fd: self.filesystem_fd,
			filesystem_path: self.filesystem_path,
			fuse_fd: self.fuse_fd,
			fuse_path: self.fuse_path,
			index: self.index.unwrap_or(0),
			isolate_root: self.unshare_all,
			mounts,
			network,
			nice: self.nice.unwrap_or(0),
			rlimit_nofile: self.rlimit_nofile,
			user_namespace_fd: self.user_namespace_fd,
		};

		// Create the backend isolation inputs.
		let backend = match backend {
			tangram_sandbox::Kind::Builtin => {
				tangram_sandbox::run::Backend::Builtin(tangram_sandbox::builtin::run::Arg {
					as_pid_1: self.as_pid_1,
					gid: self.gid.unwrap_or(0),
					hostname: self.hostname,
					root: None,
					seccomp: self.seccomp,
					uid: self.uid.unwrap_or(0),
				})
			},
			tangram_sandbox::Kind::Bubblewrap => {
				tangram_sandbox::run::Backend::Bubblewrap(tangram_sandbox::bubblewrap::run::Arg {
					as_pid_1: self.as_pid_1,
					gid: self.gid.unwrap_or(0),
					hostname: self.hostname,
					path: self.runtime_path.unwrap_or_else(|| "bwrap".into()),
					root: None,
					seccomp: self.seccomp,
					uid: self.uid.unwrap_or(0),
				})
			},
			tangram_sandbox::Kind::Gvisor => {
				tangram_sandbox::run::Backend::Gvisor(tangram_sandbox::gvisor::run::Arg {
					gid: self.gid.unwrap_or(0),
					hostname: self.hostname,
					path: self.runtime_path.unwrap_or_else(|| "runsc".into()),
					platform: self.platform.unwrap_or_default(),
					uid: self.uid.unwrap_or(0),
					..Default::default()
				})
			},
			_ => unreachable!(),
		};
		let arg = tangram_sandbox::run::Arg {
			backend,
			command,
			options,
		};

		Ok(arg)
	}

	fn command(&self) -> tg::Result<tangram_sandbox::run::Command> {
		// Collect the environment.
		let mut environment = if self.clearenv {
			std::collections::BTreeMap::new()
		} else {
			std::env::vars_os()
				.map(|(key, value)| {
					let key = key
						.into_string()
						.map_err(|_| tg::error!("environment names must be valid UTF-8"))?;
					let value = value
						.into_string()
						.map_err(|_| tg::error!("environment values must be valid UTF-8"))?;
					Ok((key, value))
				})
				.collect::<tg::Result<_>>()?
		};
		for pair in self.setenvs.as_chunks::<2>().0 {
			environment.insert(pair[0].clone(), pair[1].clone());
		}

		// Encode the arguments.
		let arguments = self
			.command
			.iter()
			.cloned()
			.map(|arg| {
				arg.into_string()
					.map_err(|_| tg::error!("launch arguments must be valid UTF-8"))
			})
			.collect::<tg::Result<_>>()?;

		// Create the command description.
		let command = tangram_sandbox::run::Command {
			args: arguments,
			cwd: self.chdir.clone().unwrap_or_else(|| "/".into()),
			env: environment,
		};

		Ok(command)
	}

	#[cfg(target_os = "linux")]
	fn cloud_hypervisor(self) -> tg::Result<tangram_sandbox::run::Arg> {
		// Create the guest command settings.
		let http2 = self.http2();
		let clearenv = self.clearenv;
		let cwd = self.chdir.clone();
		let env = self.setenvs.clone();
		let explicit_command = !self.command.is_empty();
		let command = if explicit_command {
			Some(self.command()?)
		} else {
			None
		};
		let url = self.url;
		let cgroup = self.cgroup.map(|name| tangram_sandbox::run::Cgroup {
			cpu: self.cgroup_cpu,
			memory: self.cgroup_memory,
			memory_oom_group: self.cgroup_memory_oom_group,
			memory_swap: self.cgroup_memory_swap,
			name,
			pids: self.cgroup_pids,
		});
		let options = tangram_sandbox::run::Options {
			cgroup,
			nice: self.nice.unwrap_or(0),
			rlimit_nofile: self.rlimit_nofile,
			..Default::default()
		};
		let network = match self.network.as_deref() {
			None | Some("none") => None,
			Some("passt") => Some(tangram_sandbox::cloud_hypervisor::run::NetworkKind::Passt),
			Some("tap") => Some(tangram_sandbox::cloud_hypervisor::run::NetworkKind::Tap),
			Some(kind) => return Err(tg::error!(%kind, "invalid cloud-hypervisor launch network")),
		};
		let arg = tangram_sandbox::cloud_hypervisor::run::Arg {
			cloud_hypervisor_path: self.cloud_hypervisor_path,
			create_snapshot: self.create_snapshot,
			cpu: self.cpu,
			dax: self.dax,
			dns: self.dns,
			firewall: self.firewall.unwrap_or_default(),
			guest_ip: self.guest_ip,
			host_ip: self.host_ip,
			hostname: self.hostname,
			image_path: self
				.image_path
				.ok_or_else(|| tg::error!("the image path is required"))?,
			index: self.index.unwrap_or(0),
			kernel_path: self
				.kernel_path
				.ok_or_else(|| tg::error!("the kernel path is required"))?,
			max_cpu: self.max_cpu.unwrap_or(8),
			max_memory: self.max_memory.unwrap_or(8 * 1024 * 1024 * 1024),
			memory: self.memory,
			mounts: self.mounts,
			network,
			network_interface_timeout: self
				.network_interface_timeout
				.unwrap_or(std::time::Duration::from_secs(30)),
			network_startup_timeout: self
				.network_startup_timeout
				.unwrap_or(std::time::Duration::from_secs(30)),
			path: self
				.path
				.ok_or_else(|| tg::error!("the path is required"))?,
			ports: self.ports,
			ready_timeout: self
				.ready_timeout
				.unwrap_or(std::time::Duration::from_secs(30)),
			rootfs_path: self
				.rootfs_path
				.ok_or_else(|| tg::error!("the rootfs path is required"))?,
			serial_connect_interval: self
				.serial_connect_interval
				.unwrap_or(std::time::Duration::from_millis(100)),
			serial_connect_timeout: self
				.serial_connect_timeout
				.unwrap_or(std::time::Duration::from_secs(30)),
			snapshot: self.snapshot,
			snapshot_cpu: self.snapshot_cpu.unwrap_or(1),
			snapshot_memory: self.snapshot_memory.unwrap_or(512 * 1024 * 1024),
			store_path: self
				.store_path
				.ok_or_else(|| tg::error!("the store path is required"))?,
			tangram_path: self
				.tangram_path
				.ok_or_else(|| tg::error!("the tangram path is required"))?,
		};

		// Use the explicit guest command or construct the sandbox server command.
		let mut command = if let Some(command) = command {
			command
		} else {
			let tangram_path = tangram_sandbox::Sandbox::guest_tangram_path_from_host_tangram_path(
				&arg.tangram_path,
			);
			if arg.create_snapshot.is_some() {
				tangram_sandbox::run::Command {
					args: vec![
						tangram_path.to_string_lossy().into_owned(),
						"sandbox".into(),
						"init".into(),
						"--backend".into(),
						"cloud-hypervisor".into(),
					],
					..Default::default()
				}
			} else {
				let serve = tangram_sandbox::serve::Arg {
					http2,
					library_paths: Vec::new(),
					listen: false,
					output_path: tangram_sandbox::Sandbox::guest_output_path_from_root(&arg.path),
					tangram_path,
					url: url.ok_or_else(|| tg::error!("the url is required"))?,
				};
				tangram_sandbox::run::Command::with_command(&tangram_sandbox::init::command(
					&serve,
				))?
			}
		};

		// Apply the CLI overrides to the default guest command.
		if !explicit_command {
			if clearenv {
				command.env.clear();
			}
			if let Some(cwd) = cwd {
				command.cwd = cwd;
			}
			for pair in env.as_chunks::<2>().0 {
				command.env.insert(pair[0].clone(), pair[1].clone());
			}
		}

		// Create the complete launch configuration.
		let arg = tangram_sandbox::run::Arg {
			backend: tangram_sandbox::run::Backend::CloudHypervisor(arg),
			command,
			options,
		};

		Ok(arg)
	}

	#[cfg(target_os = "linux")]
	fn http2(&self) -> tangram_http::http2::Config {
		let mut config = tangram_http::http2::Config::default();
		if let Some(value) = self.http2_connection_window_size {
			config.connection_window_size = value;
		}
		if let Some(value) = self.http2_max_concurrent_reset_streams {
			config.max_concurrent_reset_streams = value;
		}
		if let Some(value) = self.http2_max_concurrent_streams {
			config.max_concurrent_streams = Some(value);
		}
		if let Some(value) = self.http2_stream_window_size {
			config.stream_window_size = value;
		}
		config
	}
}

fn read_config(contents: &[u8]) -> tg::Result<tangram_sandbox::run::Arg> {
	serde_json::from_slice(contents)
		.map_err(|error| tg::error!(!error, "failed to parse the sandbox launch configuration"))
}

fn parse_platform(value: &str) -> tg::Result<tangram_sandbox::Platform> {
	serde_json::from_value(serde_json::Value::String(value.to_owned()))
		.map_err(|error| tg::error!(!error, "invalid gvisor platform"))
}

fn parse_seccomp(value: &str) -> tg::Result<tangram_sandbox::SeccompPolicy> {
	serde_json::from_value(serde_json::Value::String(value.to_owned()))
		.map_err(|error| tg::error!(!error, "invalid seccomp policy"))
}
