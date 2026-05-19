use {
	crate::{Sandbox, container},
	std::{
		collections::hash_map::DefaultHasher,
		ffi::{CStr, CString, OsStr},
		fmt::Write as _,
		hash::{Hash as _, Hasher as _},
		net::Ipv4Addr,
		os::{
			fd::{AsRawFd as _, FromRawFd as _, IntoRawFd as _, OwnedFd, RawFd},
			unix::{ffi::OsStrExt as _, process::CommandExt as _},
		},
		path::{Path, PathBuf},
		process::ExitCode,
		time::{Duration, Instant},
	},
	tangram_client::prelude::*,
};

pub const CLOUD_HYPERVISOR_API_SOCKET_NAME: &str = "cloud-hypervisor-api.sock";
pub const CLOUD_HYPERVISOR_VSOCK_SOCKET_NAME: &str = "cloud-hypervisor-vsock.sock";

const HOST_FS_TAG: &str = "host";
const HOST_MOUNT_POINT: &str = "/mnt/host";
const SERIAL_SOCKET_NAME: &str = "serial.sock";
const VIRTIOFSD_SOCKET_NAME: &str = "virtiofsd.sock";

const VMM_ROOT_DIR: &str = "/run/vmm";
const VMM_VIRTIOFSD_SOCKET: &str = "/run/vmm/virtiofsd.sock";
const VMM_API_SOCKET: &str = "/run/vmm/cloud-hypervisor-api.sock";
const VMM_VSOCK_SOCKET: &str = "/run/vmm/cloud-hypervisor-vsock.sock";
const VMM_SERIAL_SOCKET: &str = "/run/vmm/serial.sock";
const VMM_PASST_SOCKET: &str = "/run/vmm/passt.sock";
const VMM_KERNEL_PATH: &str = "/run/vmm/kernel";
const VMM_ROOTFS_IMAGE_PATH: &str = "/run/vmm/rootfs.img";
const VMM_SNAPSHOT_PATH: &str = "/run/vmm/snapshot";
const VMM_CLOUD_HYPERVISOR_BIN: &str = "/run/vmm/cloud-hypervisor";
const VMM_PASST_BIN: &str = "/run/vmm/passt";
const VMM_SNAPSHOT_OUTPUT_DIR: &str = "/snapshot";

const VMM_GUEST_CID: u32 = 3;

#[derive(Clone, Debug)]
pub struct Arg {
	pub artifacts_path: PathBuf,
	pub create_snapshot: Option<PathBuf>,
	pub cpu: Option<u64>,
	pub dns: Vec<Ipv4Addr>,
	pub firewall: crate::Firewall,
	pub guest_ip: Option<Ipv4Addr>,
	pub host_ip: Option<Ipv4Addr>,
	pub hostname: Option<String>,
	pub id: tg::sandbox::Id,
	pub kernel_path: PathBuf,
	pub memory: Option<u64>,
	pub mounts: Vec<tg::sandbox::Mount>,
	pub max_cpu: u64,
	pub max_memory: u64,
	pub network: Option<NetworkKind>,
	pub path: PathBuf,
	pub ports: Vec<tg::sandbox::Port>,
	pub image_path: PathBuf,
	pub rootfs_path: PathBuf,
	pub snapshot: Option<PathBuf>,
	pub snapshot_cpu: u64,
	pub snapshot_memory: u64,
	pub tangram_path: PathBuf,
	pub url: tangram_uri::Uri,
	pub user: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkKind {
	Passt,
	Tap,
}

struct VirtiofsBind {
	source: PathBuf,
	target: PathBuf,
}

impl std::str::FromStr for NetworkKind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"passt" => Ok(Self::Passt),
			"tap" => Ok(Self::Tap),
			s => Err(tg::error!(option = %s, "invalid vm network option")),
		}
	}
}

struct User {
	gid: libc::gid_t,
	home: PathBuf,
	name: String,
	uid: libc::uid_t,
}

enum Network {
	Passt(crate::network::passt::Device),
	Tap(crate::network::tap::Device),
}

struct ChildProcess {
	pid: Option<libc::pid_t>,
}

impl ChildProcess {
	fn new(pid: libc::pid_t) -> Self {
		Self { pid: Some(pid) }
	}

	fn take_if_pid(&mut self, pid: libc::pid_t) -> bool {
		if self.pid == Some(pid) {
			self.pid = None;
			true
		} else {
			false
		}
	}

	fn wait(mut self) {
		let Some(pid) = self.pid.take() else {
			return;
		};
		let mut status = 0;
		unsafe {
			libc::waitpid(pid, std::ptr::addr_of_mut!(status), 0);
		}
	}
}

impl Drop for ChildProcess {
	fn drop(&mut self) {
		let Some(pid) = self.pid.take() else {
			return;
		};
		unsafe {
			libc::kill(pid, libc::SIGKILL);
			let mut status = 0;
			libc::waitpid(pid, std::ptr::addr_of_mut!(status), 0);
		}
	}
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	if arg.cpu == Some(0) {
		return Err(tg::error!("sandbox cpu must be greater than zero"));
	}
	if arg.memory == Some(0) {
		return Err(tg::error!("sandbox memory must be greater than zero"));
	}
	if arg.snapshot_cpu == 0 {
		return Err(tg::error!("vm snapshot cpu must be greater than zero"));
	}
	if arg.snapshot_memory == 0 {
		return Err(tg::error!("vm snapshot memory must be greater than zero"));
	}
	if arg.max_cpu < arg.snapshot_cpu {
		return Err(tg::error!(
			"vm max cpu must be greater than or equal to the snapshot cpu"
		));
	}
	if arg.max_memory < arg.snapshot_memory {
		return Err(tg::error!(
			"vm max memory must be greater than or equal to the snapshot memory"
		));
	}
	let requested_cpu = arg.cpu.unwrap_or(arg.snapshot_cpu);
	if requested_cpu < arg.snapshot_cpu || requested_cpu > arg.max_cpu {
		return Err(tg::error!(
			"requested vm cpu must be within the configured snapshot resize envelope"
		));
	}
	let requested_memory = arg.memory.unwrap_or(arg.snapshot_memory);
	if requested_memory < arg.snapshot_memory || requested_memory > arg.max_memory {
		return Err(tg::error!(
			"requested vm memory must be within the configured snapshot resize envelope"
		));
	}

	if let Some(hostname) = &arg.hostname
		&& hostname.chars().any(char::is_whitespace)
	{
		return Err(tg::error!(
			%hostname,
			"hostname may not contain whitespace"
		));
	}

	tracing::trace!("sandbox directory: {}", arg.path.display());
	let user = resolve_user(arg.user.as_deref())?;
	prepare_sandbox_directory(&arg.path)?;
	prepare_etc_files(&arg.path, &user)?;

	let network = Network::new(arg)?;
	let network_config = network.as_ref().map(|net| {
		let dns_servers = match net {
			Network::Passt(passt) => vec![passt.host_ip()],
			Network::Tap(_) => arg.dns.clone(),
		};
		crate::vm::init::NetworkConfig {
			dns_servers,
			gateway_ip: net.host_ip(),
			guest_ip: net.guest_ip(),
			netmask: net.netmask(),
		}
	});
	if arg.create_snapshot.is_some() && arg.snapshot.is_some() {
		return Err(tg::error!(
			"--create-snapshot and --snapshot are mutually exclusive"
		));
	}

	if arg.create_snapshot.is_none() {
		let mounts = arg
			.mounts
			.iter()
			.enumerate()
			.map(|(index, mount)| {
				let metadata = std::fs::metadata(&mount.source).map_err(|error| {
					tg::error!(
						!error,
						source = %mount.source.display(),
						"failed to stat the user mount source",
					)
				})?;
				Ok(crate::vm::init::MountConfig {
					directory: metadata.is_dir(),
					readonly: mount.readonly,
					source: Path::new(HOST_MOUNT_POINT)
						.join("mounts")
						.join(index.to_string()),
					target: mount.target.clone(),
				})
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let init_config = crate::vm::init::Config {
			gid: user.gid,
			hostname: arg.hostname.clone(),
			library_paths: Vec::new(),
			mounts,
			network: network_config,
			output_path: Sandbox::guest_output_path_from_root(&arg.path),
			tangram_path: Sandbox::guest_tangram_path_from_host_tangram_path(&arg.tangram_path),
			uid: user.uid,
			url: arg.url.to_string(),
		};
		let init_config_bytes = serde_json::to_vec(&init_config)
			.map_err(|error| tg::error!(!error, "failed to serialize the init config"))?;
		std::fs::write(
			host_share_init_config_path_from_root(&arg.path),
			init_config_bytes,
		)
		.map_err(|error| tg::error!(!error, "failed to write the init config"))?;
	}

	let mut helpers: Vec<ChildProcess> = Vec::new();
	if arg.create_snapshot.is_none() {
		let socket_path = host_vm_path_from_root(&arg.path).join(VIRTIOFSD_SOCKET_NAME);
		tracing::trace!(
			"preparing host virtiofsd socket at {}",
			socket_path.display()
		);
		let socket = std::os::unix::net::UnixListener::bind(&socket_path)
			.map_err(|error| tg::error!(!error, "failed to open the host virtiofsd socket"))?;
		let shared_dir = host_share_path_from_root(&arg.path);

		let mut binds = Vec::new();
		binds.push(VirtiofsBind {
			source: arg.artifacts_path.clone(),
			target: host_share_artifacts_path_from_root(&arg.path),
		});
		binds.push(VirtiofsBind {
			source: Sandbox::host_tmp_path_from_root(&arg.path),
			target: host_share_tmp_path_from_root(&arg.path),
		});
		for (index, mount) in arg.mounts.iter().enumerate() {
			let target = host_share_mounts_path_from_root(&arg.path).join(index.to_string());
			let source_is_dir = mount.source.is_dir();
			if source_is_dir {
				std::fs::create_dir_all(&target).map_err(|error| {
					tg::error!(!error, path = %target.display(), "failed to create the user mount share target")
				})?;
			} else if !target.exists() {
				std::fs::File::create(&target).map_err(|error| {
					tg::error!(!error, path = %target.display(), "failed to create the user mount share target file")
				})?;
			}
			binds.push(VirtiofsBind {
				source: mount.source.clone(),
				target,
			});
		}

		tracing::trace!("spawning host virtiofsd helper");
		let pid = spawn_virtiofsd_helper(&user, &shared_dir, socket.into_raw_fd(), &binds)?;
		tracing::trace!("host virtiofsd helper pid={pid}");
		helpers.push(ChildProcess::new(pid));
	} else {
		tracing::trace!("snapshot mode: skipping virtiofsd helper");
	}

	let cloud_hypervisor_bin = find_in_path("cloud-hypervisor")
		.map_err(|error| tg::error!(!error, "failed to locate cloud-hypervisor on PATH"))?;
	tracing::trace!(
		"cloud-hypervisor binary: {}",
		cloud_hypervisor_bin.display()
	);
	let passt_bin = if matches!(arg.network, Some(NetworkKind::Passt)) {
		Some(
			find_in_path("passt")
				.map_err(|error| tg::error!(!error, "failed to locate passt on PATH"))?,
		)
	} else {
		None
	};
	if let Some(passt) = &passt_bin {
		tracing::trace!("passt binary: {}", passt.display());
	}

	let net_arg = network.as_ref().map(|net| match net {
		Network::Passt(passt) => {
			format!(
				"vhost_user=true,socket={VMM_PASST_SOCKET},mac={}",
				passt.mac()
			)
		},
		Network::Tap(tap) => tap.cloud_hypervisor_arg(),
	});

	let mut cloud_hypervisor_args: Vec<std::ffi::OsString> = Vec::new();
	cloud_hypervisor_args.push("--api-socket".into());
	cloud_hypervisor_args.push(VMM_API_SOCKET.into());
	if arg.snapshot.is_some() {
		cloud_hypervisor_args.push("--restore".into());
		cloud_hypervisor_args
			.push(format!("source_url=file://{VMM_SNAPSHOT_PATH},resume=true").into());
	} else {
		cloud_hypervisor_args.push("--kernel".into());
		cloud_hypervisor_args.push(VMM_KERNEL_PATH.into());
		cloud_hypervisor_args.push("--cpus".into());
		cloud_hypervisor_args.push(format!("boot={},max={}", arg.snapshot_cpu, arg.max_cpu).into());
		cloud_hypervisor_args.push("--memory".into());
		cloud_hypervisor_args.push(memory_arg(arg).into());
		cloud_hypervisor_args.push("--cmdline".into());
		cloud_hypervisor_args.push(kernel_cmdline(arg).into());
		cloud_hypervisor_args.push("--disk".into());
		cloud_hypervisor_args.push(format!("path={VMM_ROOTFS_IMAGE_PATH},readonly=on").into());
		if arg.create_snapshot.is_none() {
			cloud_hypervisor_args.push("--fs".into());
			cloud_hypervisor_args
				.push(format!("tag={HOST_FS_TAG},socket={VMM_VIRTIOFSD_SOCKET}").into());
		}
		cloud_hypervisor_args.push("--vsock".into());
		cloud_hypervisor_args.push(format!("cid={VMM_GUEST_CID},socket={VMM_VSOCK_SOCKET}").into());
		cloud_hypervisor_args.push("--console".into());
		cloud_hypervisor_args.push("off".into());
		cloud_hypervisor_args.push("--serial".into());
		cloud_hypervisor_args.push(format!("socket={VMM_SERIAL_SOCKET}").into());
		if let Some(net) = &net_arg {
			cloud_hypervisor_args.push("--net".into());
			cloud_hypervisor_args.push(net.clone().into());
		}
	}

	tracing::trace!("cloud-hypervisor args: {:?}", cloud_hypervisor_args);
	tracing::trace!("spawning cloud-hypervisor helper");
	let cloud_hypervisor_pid = spawn_cloud_hypervisor_helper(
		arg,
		&user,
		&cloud_hypervisor_bin,
		passt_bin.as_deref(),
		&cloud_hypervisor_args,
	)?;
	tracing::trace!("cloud-hypervisor helper pid={cloud_hypervisor_pid}");
	let mut cloud_hypervisor = ChildProcess::new(cloud_hypervisor_pid);

	let serial_socket_path = host_vm_path_from_root(&arg.path).join(SERIAL_SOCKET_NAME);
	let serial_stream = connect_serial_socket(&serial_socket_path, Duration::from_secs(10))?;
	tracing::trace!("connected to guest serial socket; tailing to stderr");
	let serial_reader = serial_stream
		.try_clone()
		.map_err(|error| tg::error!(!error, "failed to clone the serial socket"))?;
	let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<()>(1);
	std::thread::spawn(move || {
		let mut stream = serial_reader;
		let mut buf = [0u8; 4096];
		let mut signaled = false;
		let stderr = std::io::stderr();
		loop {
			match std::io::Read::read(&mut stream, &mut buf) {
				Ok(0) | Err(_) => return,
				Ok(n) => {
					let chunk = &buf[..n];
					{
						let mut handle = stderr.lock();
						let _ = std::io::Write::write_all(&mut handle, chunk);
						let _ = std::io::Write::flush(&mut handle);
					}
					if !signaled && chunk.contains(&0u8) {
						let _ = ready_tx.try_send(());
						signaled = true;
					}
				},
			}
		}
	});

	if let Some(snapshot_output) = arg.create_snapshot.as_ref() {
		let api_socket = host_vm_path_from_root(&arg.path).join(CLOUD_HYPERVISOR_API_SOCKET_NAME);
		let ch_remote_bin = find_in_path("ch-remote")
			.map_err(|error| tg::error!(!error, "failed to locate ch-remote on PATH"))?;

		if ready_rx.recv_timeout(Duration::from_mins(1)).is_err() {
			return Err(tg::error!("timed out waiting for the guest ready signal"));
		}

		ch_remote_run(&ch_remote_bin, &api_socket, &["pause"])?;
		ch_remote_run(
			&ch_remote_bin,
			&api_socket,
			&["snapshot", "file:///snapshot"],
		)?;
		ch_remote_run(&ch_remote_bin, &api_socket, &["shutdown-vmm"])?;
		cloud_hypervisor.wait();

		let snapshot_dir = host_snapshot_output_path_from_root(&arg.path);
		if let Some(parent) = snapshot_output.parent()
			&& !parent.as_os_str().is_empty()
		{
			std::fs::create_dir_all(parent).map_err(|error| {
				tg::error!(
					!error,
					path = %parent.display(),
					"failed to create the snapshot parent directory",
				)
			})?;
		}
		std::fs::remove_dir_all(snapshot_output).ok();
		std::fs::remove_file(snapshot_output).ok();
		match std::fs::rename(&snapshot_dir, snapshot_output) {
			Ok(()) => {},
			Err(error) if error.raw_os_error() == Some(libc::EXDEV) => {
				copy_directory(&snapshot_dir, snapshot_output)?;
				std::fs::remove_dir_all(&snapshot_dir).map_err(|error| {
					tg::error!(
						!error,
						path = %snapshot_dir.display(),
						"failed to remove the snapshot directory",
					)
				})?;
			},
			Err(error) => {
				return Err(tg::error!(
					!error,
					src = %snapshot_dir.display(),
					dst = %snapshot_output.display(),
					"failed to move the snapshot",
				));
			},
		}
		return Ok(ExitCode::SUCCESS);
	}

	if arg.snapshot.is_some() {
		let api_socket = host_vm_path_from_root(&arg.path).join(CLOUD_HYPERVISOR_API_SOCKET_NAME);
		let ch_remote_bin = find_in_path("ch-remote")
			.map_err(|error| tg::error!(!error, "failed to locate ch-remote on PATH"))?;
		if requested_cpu != arg.snapshot_cpu {
			let requested_cpu = requested_cpu.to_string();
			ch_remote_run(
				&ch_remote_bin,
				&api_socket,
				&["resize", "--cpus", &requested_cpu],
			)?;
		}
		if requested_memory != arg.snapshot_memory {
			let requested_memory = requested_memory.to_string();
			ch_remote_run(
				&ch_remote_bin,
				&api_socket,
				&["resize", "--memory", &requested_memory],
			)?;
		}
		if let Some(net) = &net_arg {
			ch_remote_run(&ch_remote_bin, &api_socket, &["add-net", net])?;
		}
		ch_remote_run(
			&ch_remote_bin,
			&api_socket,
			&[
				"add-fs",
				&format!("tag={HOST_FS_TAG},socket={VMM_VIRTIOFSD_SOCKET}"),
			],
		)?;
	}

	wait_for_cloud_hypervisor(&mut cloud_hypervisor, &mut helpers)
}

fn connect_serial_socket(
	path: &Path,
	timeout: Duration,
) -> tg::Result<std::os::unix::net::UnixStream> {
	let parent = path.parent().ok_or_else(
		|| tg::error!(path = %path.display(), "the serial socket path has no parent"),
	)?;
	let inotify = watch_directory(parent)?;
	let deadline = Instant::now() + timeout;
	loop {
		match std::os::unix::net::UnixStream::connect(path) {
			Ok(stream) => return Ok(stream),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				wait_for_directory_event(&inotify, deadline).map_err(|wait_error| {
					tg::error!(
						source = wait_error,
						!error,
						path = %path.display(),
						"timed out waiting for the cloud-hypervisor serial socket",
					)
				})?;
			},
			Err(error) => {
				return Err(tg::error!(
					!error,
					path = %path.display(),
					"failed to connect to the guest serial socket",
				));
			},
		}
	}
}

fn wait_for_cloud_hypervisor(
	cloud_hypervisor: &mut ChildProcess,
	helpers: &mut [ChildProcess],
) -> tg::Result<ExitCode> {
	loop {
		let mut status = 0;
		let pid = unsafe { libc::waitpid(-1, std::ptr::addr_of_mut!(status), 0) };
		if pid < 0 {
			let error = std::io::Error::last_os_error();
			if error.raw_os_error() == Some(libc::EINTR) {
				continue;
			}
			return Err(tg::error!(!error, "failed to wait for a child process"));
		}
		if cloud_hypervisor.take_if_pid(pid) {
			return Ok(ExitCode::from(exit_code_from_status(status)));
		}
		for helper in helpers.iter_mut() {
			if helper.take_if_pid(pid) {
				return Err(tg::error!(
					helper_status = %exit_code_from_status(status),
					"the virtiofsd helper exited unexpectedly"
				));
			}
		}
	}
}

fn watch_directory(path: &Path) -> tg::Result<OwnedFd> {
	let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
	if fd < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to create the directory watcher"));
	}
	let fd = unsafe { OwnedFd::from_raw_fd(fd) };
	let path = CString::new(path.as_os_str().as_bytes())
		.map_err(|error| tg::error!(!error, "failed to encode the directory path"))?;
	let mask = libc::IN_CREATE | libc::IN_MOVED_TO | libc::IN_ATTRIB;
	let result = unsafe { libc::inotify_add_watch(fd.as_raw_fd(), path.as_ptr(), mask) };
	if result < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to watch the directory"));
	}
	Ok(fd)
}

fn wait_for_directory_event(inotify: &OwnedFd, deadline: Instant) -> tg::Result<()> {
	let now = Instant::now();
	if now >= deadline {
		return Err(tg::error!("timed out"));
	}
	let timeout = deadline.duration_since(now).as_millis();
	let timeout = i32::try_from(timeout).unwrap_or(i32::MAX);
	let mut event = libc::pollfd {
		fd: inotify.as_raw_fd(),
		events: libc::POLLIN,
		revents: 0,
	};
	let result = unsafe { libc::poll(std::ptr::addr_of_mut!(event), 1, timeout) };
	if result < 0 {
		let error = std::io::Error::last_os_error();
		if error.raw_os_error() == Some(libc::EINTR) {
			return Ok(());
		}
		return Err(tg::error!(
			!error,
			"failed to wait for the directory watcher"
		));
	}
	if result == 0 {
		return Err(tg::error!("timed out"));
	}
	let mut buffer = [0u8; 4096];
	loop {
		let result = unsafe {
			libc::read(
				inotify.as_raw_fd(),
				buffer.as_mut_ptr().cast(),
				buffer.len(),
			)
		};
		if result > 0 {
			continue;
		}
		if result == 0 {
			return Ok(());
		}
		let error = std::io::Error::last_os_error();
		if error.raw_os_error() == Some(libc::EAGAIN) {
			return Ok(());
		}
		if error.raw_os_error() == Some(libc::EINTR) {
			continue;
		}
		return Err(tg::error!(
			!error,
			"failed to read from the directory watcher"
		));
	}
}

fn helper_child_main(
	user: &User,
	shared_dir: &Path,
	socket: RawFd,
	binds: &[VirtiofsBind],
) -> tg::Result<()> {
	set_parent_death_signal(libc::SIGKILL)?;
	enter_user_namespace(user.uid, user.gid)?;
	unshare(
		libc::CLONE_NEWNS | libc::CLONE_NEWIPC,
		"failed to unshare the sandbox namespaces",
	)?;
	unshare(
		libc::CLONE_NEWNET,
		"failed to unshare the network namespace",
	)?;

	let root = std::ffi::CString::new("/").unwrap();
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

	for bind in binds {
		let source_c = std::ffi::CString::new(bind.source.as_os_str().as_bytes())
			.map_err(|error| tg::error!(!error, "failed to encode the bind source path"))?;
		let target_c = std::ffi::CString::new(bind.target.as_os_str().as_bytes())
			.map_err(|error| tg::error!(!error, "failed to encode the bind target path"))?;
		let result = unsafe {
			libc::mount(
				source_c.as_ptr(),
				target_c.as_ptr(),
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
				"failed to bind the host share entry",
			));
		}
	}
	let virtiofsd_path =
		find_virtiofsd().map_err(|source| tg::error!(!source, "failed to locate virtiofsd"))?;

	set_no_new_privs()?;
	setresgid(user.gid)?;
	setresuid(user.uid)?;

	let mut command = std::process::Command::new(virtiofsd_path);
	command
		.arg("--shared-dir")
		.arg(shared_dir)
		.arg("--fd")
		.arg(socket.to_string())
		.arg("--sandbox")
		.arg("none")
		.arg("--cache")
		.arg("always")
		.arg("--inode-file-handles=never")
		.arg("--xattr")
		.arg("--log-level")
		.arg("warn")
		.arg("--no-announce-submounts")
		.arg("--preserve-noatime")
		.env("HOME", &user.home)
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit());
	unsafe {
		command.pre_exec(move || {
			let flags = libc::fcntl(socket, libc::F_GETFD);
			if flags < 0 {
				return Err(std::io::Error::last_os_error());
			}
			if libc::fcntl(socket, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok(())
		});
	}
	let error = command.exec();
	Err(tg::error!(!error, "failed to execute virtiofsd"))
}

fn enter_user_namespace(uid: libc::uid_t, gid: libc::gid_t) -> tg::Result<()> {
	let host_uid = unsafe { libc::getuid() };
	let host_gid = unsafe { libc::getgid() };
	unshare(libc::CLONE_NEWUSER, "failed to unshare the user namespace")?;
	std::fs::write("/proc/self/uid_map", format!("{uid} {host_uid} 1\n"))
		.map_err(|error| tg::error!(!error, "failed to write the uid map"))?;
	std::fs::write("/proc/self/setgroups", "deny")
		.map_err(|error| tg::error!(!error, "failed to deny setgroups"))?;
	std::fs::write("/proc/self/gid_map", format!("{gid} {host_gid} 1\n"))
		.map_err(|error| tg::error!(!error, "failed to write the gid map"))?;
	Ok(())
}

fn host_vm_path_from_root(root_path: &Path) -> PathBuf {
	root_path.join("vm")
}

fn host_share_path_from_root(root_path: &Path) -> PathBuf {
	root_path.join("host")
}

fn host_share_init_config_path_from_root(root_path: &Path) -> PathBuf {
	host_share_path_from_root(root_path).join("init.json")
}

fn host_share_etc_path_from_root(root_path: &Path) -> PathBuf {
	host_share_path_from_root(root_path).join("etc")
}

fn host_share_passwd_path_from_root(root_path: &Path) -> PathBuf {
	host_share_etc_path_from_root(root_path).join("passwd")
}

fn host_share_nsswitch_path_from_root(root_path: &Path) -> PathBuf {
	host_share_etc_path_from_root(root_path).join("nsswitch.conf")
}

fn host_share_opt_tangram_path_from_root(root_path: &Path) -> PathBuf {
	host_share_path_from_root(root_path).join("opt/tangram")
}

fn host_share_artifacts_path_from_root(root_path: &Path) -> PathBuf {
	host_share_opt_tangram_path_from_root(root_path).join("artifacts")
}

fn host_share_output_path_from_root(root_path: &Path) -> PathBuf {
	host_share_opt_tangram_path_from_root(root_path).join("output")
}

fn host_share_tmp_path_from_root(root_path: &Path) -> PathBuf {
	host_share_path_from_root(root_path).join("tmp")
}

fn host_share_mounts_path_from_root(root_path: &Path) -> PathBuf {
	host_share_path_from_root(root_path).join("mounts")
}

fn kernel_cmdline(arg: &Arg) -> String {
	let tangram_path = Sandbox::guest_tangram_path_from_host_tangram_path(&arg.tangram_path);
	let mut cmdline =
		String::from("console=ttyS0 quiet loglevel=0 root=/dev/vda rootfstype=squashfs ro");
	write!(
		&mut cmdline,
		" init={} -- sandbox vm init",
		tangram_path.display(),
	)
	.unwrap();
	cmdline
}

fn memory_arg(arg: &Arg) -> String {
	let mut memory = format!("size={},shared=on", arg.snapshot_memory);
	let hotplug_size = arg.max_memory - arg.snapshot_memory;
	if hotplug_size > 0 {
		write!(&mut memory, ",hotplug_size={hotplug_size}").unwrap();
	}
	memory
}

fn prepare_sandbox_directory(sandbox_path: &Path) -> tg::Result<()> {
	for path in [
		Sandbox::host_scratch_path_from_root(sandbox_path),
		Sandbox::host_upper_path_from_root(sandbox_path),
		Sandbox::host_work_path_from_root(sandbox_path),
		host_vm_path_from_root(sandbox_path),
		host_share_path_from_root(sandbox_path),
		host_share_etc_path_from_root(sandbox_path),
		host_share_opt_tangram_path_from_root(sandbox_path),
		host_share_output_path_from_root(sandbox_path),
		host_share_artifacts_path_from_root(sandbox_path),
		host_share_tmp_path_from_root(sandbox_path),
		host_share_mounts_path_from_root(sandbox_path),
	] {
		std::fs::create_dir_all(&path).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the sandbox path"),
		)?;
	}

	let host_output = Sandbox::host_output_path_from_root(sandbox_path);
	std::fs::remove_file(&host_output).ok();
	std::fs::remove_dir_all(&host_output).ok();
	std::os::unix::fs::symlink("host/opt/tangram/output", &host_output).map_err(
		|error| tg::error!(!error, path = %host_output.display(), "failed to symlink the output dir"),
	)?;

	let host_tmp = Sandbox::host_tmp_path_from_root(sandbox_path);
	std::fs::remove_file(&host_tmp).ok();
	std::fs::remove_dir_all(&host_tmp).ok();
	std::os::unix::fs::symlink("host/tmp", &host_tmp).map_err(
		|error| tg::error!(!error, path = %host_tmp.display(), "failed to symlink the tmp dir"),
	)?;

	let permissions =
		<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o1777);
	let share_tmp = host_share_tmp_path_from_root(sandbox_path);
	std::fs::set_permissions(&share_tmp, permissions).map_err(|error| {
		tg::error!(
			!error,
			path = %share_tmp.display(),
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
	for path in [
		host_vm_path_from_root(sandbox_path).join(VIRTIOFSD_SOCKET_NAME),
		host_vm_path_from_root(sandbox_path).join(CLOUD_HYPERVISOR_VSOCK_SOCKET_NAME),
		host_vm_path_from_root(sandbox_path).join(SERIAL_SOCKET_NAME),
		host_vm_path_from_root(sandbox_path).join(CLOUD_HYPERVISOR_API_SOCKET_NAME),
	] {
		std::fs::remove_file(path).ok();
	}
	Ok(())
}

fn prepare_etc_files(sandbox_path: &Path, user: &User) -> tg::Result<()> {
	let mut passwd = String::from(
		"root:!:0:0:root:/root:/bin/false\nnobody:!:65534:65534:nobody:/nonexistent:/bin/false\n",
	);
	if user.uid != 0 && user.uid != 65534 {
		use std::fmt::Write as _;
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
	std::fs::write(host_share_passwd_path_from_root(sandbox_path), passwd)
		.map_err(|error| tg::error!(!error, "failed to write /etc/passwd"))?;
	let nsswitch = indoc::indoc!(
		"
			passwd: files compat
			shadow: files compat
			hosts: files dns compat
		"
	);
	std::fs::write(host_share_nsswitch_path_from_root(sandbox_path), nsswitch)
		.map_err(|error| tg::error!(!error, "failed to write /etc/nsswitch.conf"))?;
	Ok(())
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
	Ok(User {
		gid: passwd.pw_gid,
		home: PathBuf::from(home),
		name,
		uid: passwd.pw_uid,
	})
}

fn set_no_new_privs() -> tg::Result<()> {
	let result = unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set no_new_privs"));
	}
	Ok(())
}

fn set_parent_death_signal(signal: libc::c_int) -> tg::Result<()> {
	let result = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, signal, 0, 0, 0) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set the parent death signal"));
	}
	Ok(())
}

fn setresgid(gid: libc::gid_t) -> tg::Result<()> {
	let result = unsafe { libc::setresgid(gid, gid, gid) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set the gid"));
	}
	Ok(())
}

fn setresuid(uid: libc::uid_t) -> tg::Result<()> {
	let result = unsafe { libc::setresuid(uid, uid, uid) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set the uid"));
	}
	Ok(())
}

fn spawn_virtiofsd_helper(
	user: &User,
	shared_dir: &Path,
	socket: RawFd,
	binds: &[VirtiofsBind],
) -> tg::Result<libc::pid_t> {
	let child = unsafe { libc::fork() };
	if child < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to fork the virtiofsd helper"));
	}
	if child == 0 {
		match helper_child_main(user, shared_dir, socket, binds) {
			Ok(()) => std::process::exit(0),
			Err(error) => {
				eprintln!("{error}");
				eprintln!("{}", error.trace());
				std::process::exit(105);
			},
		}
	}
	Ok(child)
}

fn spawn_cloud_hypervisor_helper(
	arg: &Arg,
	user: &User,
	cloud_hypervisor_bin: &Path,
	passt_bin: Option<&Path>,
	command_args: &[std::ffi::OsString],
) -> tg::Result<libc::pid_t> {
	let child = unsafe { libc::fork() };
	if child < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			"failed to fork the cloud-hypervisor helper"
		));
	}
	if child == 0 {
		match cloud_hypervisor_child_main(arg, user, cloud_hypervisor_bin, passt_bin, command_args)
		{
			Ok(()) => std::process::exit(0),
			Err(error) => {
				eprintln!("{error}");
				eprintln!("{}", error.trace());
				std::process::exit(105);
			},
		}
	}
	Ok(child)
}

fn cloud_hypervisor_child_main(
	arg: &Arg,
	user: &User,
	cloud_hypervisor_bin: &Path,
	passt_bin: Option<&Path>,
	command_args: &[std::ffi::OsString],
) -> tg::Result<()> {
	set_parent_death_signal(libc::SIGKILL)?;
	enter_user_namespace(user.uid, user.gid)?;
	unshare(
		libc::CLONE_NEWNS,
		"failed to unshare the cloud-hypervisor mount namespace",
	)?;

	let chroot = host_vmm_chroot_path_from_root(&arg.path);
	std::fs::remove_dir_all(&chroot).ok();
	std::fs::create_dir_all(&chroot).map_err(
		|error| tg::error!(!error, path = %chroot.display(), "failed to create the chroot path"),
	)?;
	if arg.create_snapshot.is_some() {
		let snapshot_output = host_snapshot_output_path_from_root(&arg.path);
		std::fs::create_dir_all(&snapshot_output).map_err(|error| {
			tg::error!(!error, path = %snapshot_output.display(), "failed to create the snapshot output directory")
		})?;
	}

	let mount_arg = build_cloud_hypervisor_mount_arg(arg, cloud_hypervisor_bin, passt_bin);
	container::mount::apply(&mount_arg, Some(&chroot))?;
	set_no_new_privs()?;

	container::mount::change_directory(&chroot)?;
	let result = unsafe { libc::chroot(c".".as_ptr()) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "chroot failed"));
	}
	container::mount::change_directory(Path::new("/"))?;

	setresgid(user.gid)?;
	setresuid(user.uid)?;

	let mut command = std::process::Command::new(VMM_CLOUD_HYPERVISOR_BIN);
	command
		.args(command_args)
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit());
	let error = command.exec();
	Err(tg::error!(!error, "failed to execute cloud-hypervisor"))
}

fn build_cloud_hypervisor_mount_arg(
	arg: &Arg,
	cloud_hypervisor_bin: &Path,
	passt_bin: Option<&Path>,
) -> container::run::Arg {
	let mut binds = vec![
		container::run::Bind {
			source: host_vm_path_from_root(&arg.path),
			target: VMM_ROOT_DIR.into(),
		},
		container::run::Bind {
			source: "/dev".into(),
			target: "/dev".into(),
		},
		container::run::Bind {
			source: "/proc".into(),
			target: "/proc".into(),
		},
	];
	if arg.create_snapshot.is_some() {
		binds.push(container::run::Bind {
			source: host_snapshot_output_path_from_root(&arg.path),
			target: VMM_SNAPSHOT_OUTPUT_DIR.into(),
		});
	}
	let mut ro_binds = vec![
		container::run::Bind {
			source: arg.rootfs_path.clone(),
			target: "/".into(),
		},
		container::run::Bind {
			source: "/sys".into(),
			target: "/sys".into(),
		},
		container::run::Bind {
			source: arg.kernel_path.clone(),
			target: VMM_KERNEL_PATH.into(),
		},
		container::run::Bind {
			source: arg.image_path.clone(),
			target: VMM_ROOTFS_IMAGE_PATH.into(),
		},
		container::run::Bind {
			source: cloud_hypervisor_bin.to_owned(),
			target: VMM_CLOUD_HYPERVISOR_BIN.into(),
		},
	];
	if let Some(passt_bin) = passt_bin {
		ro_binds.push(container::run::Bind {
			source: passt_bin.to_owned(),
			target: VMM_PASST_BIN.into(),
		});
	}
	if let Some(snapshot) = &arg.snapshot {
		ro_binds.push(container::run::Bind {
			source: snapshot.clone(),
			target: VMM_SNAPSHOT_PATH.into(),
		});
	}
	container::run::Arg {
		as_pid_1: false,
		binds,
		cgroup: None,
		cgroup_cpu: None,
		cgroup_memory: None,
		cgroup_memory_oom_group: false,
		chdir: "/".into(),
		command: Vec::new(),
		devs: Vec::new(),
		die_with_parent: false,
		gateway_ip: None,
		gid: 0,
		guest_ip: None,
		hostname: None,
		id: arg.id.clone(),
		network: None,
		network_fd: None,
		new_session: false,
		nice: 0,
		overlay_sources: Vec::new(),
		overlays: Vec::new(),
		procs: Vec::new(),
		ro_binds,
		setenvs: Vec::new(),
		tmpfs: Vec::new(),
		uid: 0,
		unshare_all: false,
	}
}

fn ch_remote_run(bin: &Path, api_socket: &Path, args: &[&str]) -> tg::Result<()> {
	let socket_arg = format!("--api-socket={}", api_socket.display());
	let status = std::process::Command::new(bin)
		.arg(&socket_arg)
		.args(args)
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit())
		.status()
		.map_err(|error| tg::error!(!error, "failed to invoke ch-remote"))?;
	if !status.success() {
		return Err(tg::error!(
			%status,
			command = ?args,
			"ch-remote command failed",
		));
	}
	Ok(())
}

fn copy_directory(src: &Path, dst: &Path) -> tg::Result<()> {
	std::fs::create_dir_all(dst).map_err(|error| {
		tg::error!(
			!error,
			path = %dst.display(),
			"failed to create the destination directory",
		)
	})?;
	for entry in std::fs::read_dir(src).map_err(
		|error| tg::error!(!error, path = %src.display(), "failed to read the source directory"),
	)? {
		let entry =
			entry.map_err(|error| tg::error!(!error, "failed to read a directory entry"))?;
		let file_type = entry
			.file_type()
			.map_err(|error| tg::error!(!error, "failed to read a directory entry type"))?;
		let from = entry.path();
		let to = dst.join(entry.file_name());
		if file_type.is_dir() {
			copy_directory(&from, &to)?;
		} else {
			std::fs::copy(&from, &to).map_err(
				|error| tg::error!(!error, src = %from.display(), dst = %to.display(), "failed to copy a file"),
			)?;
		}
	}
	Ok(())
}

fn find_in_path(name: &str) -> tg::Result<PathBuf> {
	let path = std::env::var_os("PATH")
		.ok_or_else(|| tg::error!("PATH environment variable is not set"))?;
	for dir in std::env::split_paths(&path) {
		let candidate = dir.join(name);
		if let Ok(meta) = std::fs::metadata(&candidate)
			&& meta.is_file()
		{
			return Ok(candidate);
		}
	}
	Err(tg::error!(%name, "executable not found on PATH"))
}

fn find_virtiofsd() -> tg::Result<PathBuf> {
	if let Ok(path) = find_in_path("virtiofsd") {
		return Ok(path);
	}
	for path in ["/usr/lib/virtiofsd", "/usr/libexec/virtiofsd"] {
		let path = PathBuf::from(path);
		if path.is_file() {
			return Ok(path);
		}
	}
	Err(tg::error!("virtiofsd not found"))
}

fn host_snapshot_output_path_from_root(root_path: &Path) -> PathBuf {
	root_path.join("snapshot")
}

fn host_vmm_chroot_path_from_root(root_path: &Path) -> PathBuf {
	root_path.join("vmm-chroot/root")
}

fn unshare(flags: libc::c_int, message: &'static str) -> tg::Result<()> {
	let result = unsafe { libc::unshare(flags) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "{}", message));
	}
	Ok(())
}

fn exit_code_from_status(status: libc::c_int) -> u8 {
	if libc::WIFEXITED(status) {
		return u8::try_from(libc::WEXITSTATUS(status).min(255)).unwrap_or(u8::MAX);
	}
	if libc::WIFSIGNALED(status) {
		let signal = libc::WTERMSIG(status);
		return u8::try_from((128 + signal).min(255)).unwrap_or(u8::MAX);
	}
	1
}

impl Network {
	fn new(arg: &Arg) -> tg::Result<Option<Self>> {
		match &arg.network {
			Some(NetworkKind::Passt) => {
				let host_ip = arg
					.host_ip
					.ok_or_else(|| tg::error!("--host-ip is required when --network is set"))?;
				let guest_ip = arg
					.guest_ip
					.ok_or_else(|| tg::error!("--guest-ip is required when --network is set"))?;
				let passt = crate::network::passt::Device::new(
					&host_vm_path_from_root(&arg.path),
					&arg.dns,
					host_ip,
					guest_ip,
					&arg.ports,
				)?;
				Ok(Some(Network::Passt(passt)))
			},
			Some(NetworkKind::Tap) => {
				let host_ip = arg
					.host_ip
					.ok_or_else(|| tg::error!("--host-ip is required when --network is set"))?;
				let guest_ip = arg
					.guest_ip
					.ok_or_else(|| tg::error!("--guest-ip is required when --network is set"))?;
				let mut hasher = DefaultHasher::new();
				arg.path.hash(&mut hasher);
				let id = format!("{:x}", hasher.finish());
				let tap = crate::network::tap::Device::new(&id, arg.firewall, host_ip, guest_ip)?;
				Ok(Some(Network::Tap(tap)))
			},
			None => Ok(None),
		}
	}

	fn host_ip(&self) -> Ipv4Addr {
		match self {
			Self::Passt(passt) => passt.host_ip(),
			Self::Tap(tap) => tap.host_ip(),
		}
	}

	fn guest_ip(&self) -> Ipv4Addr {
		match self {
			Self::Passt(passt) => passt.guest_ip(),
			Self::Tap(tap) => tap.guest_ip(),
		}
	}

	fn netmask(&self) -> Ipv4Addr {
		match self {
			Self::Passt(passt) => passt.netmask(),
			Self::Tap(tap) => tap.netmask(),
		}
	}
}
