use super::{
	proxy::{self, Proxy},
	util::render,
};
use crate::{temp::Temp, Server};
use bytes::Bytes;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	FutureExt as _, TryStreamExt as _,
};
use indoc::formatdoc;
use itertools::Itertools as _;
use std::{
	collections::BTreeMap,
	ffi::CString,
	os::{fd::AsRawFd, unix::ffi::OsStrExt as _},
	path::{Path, PathBuf},
};
use tangram_client as tg;
use tangram_futures::task::Task;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use url::Url;

/// The home directory guest path.
const HOME_DIRECTORY_GUEST_PATH: &str = "/home/tangram";

/// The output parent directory guest path.
const OUTPUT_PARENT_DIRECTORY_GUEST_PATH: &str = "/output";

/// The server guest path.
const SERVER_DIRECTORY_GUEST_PATH: &str = "/.tangram";

/// The GID for the tangram user.
const TANGRAM_GID: libc::gid_t = 1000;

/// The UID for the tangram user.
const TANGRAM_UID: libc::uid_t = 1000;

/// The working directory guest path.
const WORKING_DIRECTORY_GUEST_PATH: &str = "/home/tangram/work";

#[cfg(target_arch = "aarch64")]
const DASH: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/dash_aarch64_linux"));

#[cfg(target_arch = "x86_64")]
const DASH: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/dash_x86_64_linux"));

#[cfg(target_arch = "aarch64")]
const ENV: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/env_aarch64_linux"));

#[cfg(target_arch = "x86_64")]
const ENV: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/env_x86_64_linux"));

#[derive(Clone)]
pub struct Runtime {
	server: Server,
	env: tg::File,
	sh: tg::File,
}

impl Runtime {
	pub async fn new(server: &Server) -> tg::Result<Self> {
		let env = tg::Blob::with_reader(server, ENV).await?;
		let env = tg::File::builder(env).executable(true).build();
		let sh = tg::Blob::with_reader(server, DASH).await?;
		let sh = tg::File::builder(sh).executable(true).build();
		let server = server.clone();
		Ok(Self { server, env, sh })
	}

	pub async fn build(&self, build: &tg::Build, remote: Option<String>) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Get the target.
		let target = build.target(server).await?;

		// Get the checksum.
		let checksum = target.checksum(server).await?;

		// Try to reuse a build whose checksum is `None` or `Unsafe`.
		if let Ok(value) =
			super::util::try_reuse_build(server, build.id(), &target, checksum.as_ref())
				.boxed()
				.await
		{
			return Ok(value);
		};

		// If the VFS is disabled, then check out the target's children.
		if server.vfs.lock().unwrap().is_none() {
			target
				.data(server)
				.await?
				.children()
				.into_iter()
				.filter_map(|id| id.try_into().ok())
				.map(tg::Artifact::with_id)
				.chain([self.env.clone().into(), self.sh.clone().into()])
				.map(|artifact| async move {
					let arg = tg::artifact::checkout::Arg::default();
					artifact.check_out(server, arg).await?;
					Ok::<_, tg::Error>(())
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await?;
		}

		// Get the server directory path.
		let server_directory_host_path = server.config.path.clone();
		let server_directory_guest_path = PathBuf::from(SERVER_DIRECTORY_GUEST_PATH);

		// Create a tempdir for the root.
		let root_directory_temp = Temp::new(server);
		tokio::fs::create_dir_all(&root_directory_temp)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to create the root temporary directory")
			})?;
		let root_directory_host_path = PathBuf::from(root_directory_temp.as_ref());

		// Create a tempdir for the output.
		let output_parent_directory_temp = Temp::new(server);
		tokio::fs::create_dir_all(&output_parent_directory_temp)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to create the output parent directory")
			})?;

		// Create the host and guest paths for the output parent directory.
		let output_parent_directory_host_path =
			PathBuf::from(output_parent_directory_temp.as_ref());
		let output_parent_directory_guest_path = PathBuf::from(OUTPUT_PARENT_DIRECTORY_GUEST_PATH);

		// Create the host and guest paths for the output.
		let output_host_path = output_parent_directory_host_path.join("output");
		let output_guest_path = output_parent_directory_guest_path.join("output");

		// Create the host and guest paths for the artifacts directory.
		let _artifacts_directory_host_path = server_directory_host_path.join("artifacts");
		let artifacts_directory_guest_path = server_directory_guest_path.join("artifacts");

		// Create symlinks for /usr/bin/env and /bin/sh.
		let env_path = root_directory_host_path.join("usr/bin/env");
		let sh_path = root_directory_host_path.join("bin/sh");
		tokio::fs::create_dir_all(&env_path.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
		tokio::fs::create_dir_all(&sh_path.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
		let env_guest_path =
			artifacts_directory_guest_path.join(self.env.id(server).await?.to_string());
		let sh_guest_path =
			artifacts_directory_guest_path.join(self.sh.id(server).await?.to_string());

		tokio::fs::symlink(&env_guest_path, &env_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the env symlink"))?;
		tokio::fs::symlink(&sh_guest_path, &sh_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the sh symlink"))?;

		// Create the host and guest paths for the home directory, with inner .tangram directory.
		let home_directory_host_path =
			root_directory_host_path.join(HOME_DIRECTORY_GUEST_PATH.strip_prefix('/').unwrap());
		let home_directory_guest_path = PathBuf::from(HOME_DIRECTORY_GUEST_PATH);
		tokio::fs::create_dir_all(&home_directory_host_path.join(".tangram"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the home directory"))?;

		// Create the host and guest paths for the working directory.
		let working_directory_host_path =
			root_directory_host_path.join(WORKING_DIRECTORY_GUEST_PATH.strip_prefix('/').unwrap());
		tokio::fs::create_dir_all(&working_directory_host_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the working directory"))?;

		// Create the path map.
		let path_map = proxy::PathMap {
			output_host: output_parent_directory_host_path.clone(),
			output_guest: output_parent_directory_guest_path.clone(),
			root_host: root_directory_host_path.clone(),
		};

		// Create the proxy server host URL.
		let proxy_server_socket_guest_path = home_directory_guest_path.join(".tangram/socket");
		let proxy_server_socket_guest_path =
			urlencoding::encode(proxy_server_socket_guest_path.to_str().ok_or_else(
				|| tg::error!(%path = proxy_server_socket_guest_path.display(), "invalid path"),
			)?);
		let proxy_server_guest_url = format!("http+unix://{proxy_server_socket_guest_path}");
		let proxy_server_guest_url = Url::parse(&proxy_server_guest_url)
			.map_err(|source| tg::error!(!source, "failed to parse the proxy server url"))?;

		// Create the proxy server guest URL.
		let proxy_server_socket_host_path = home_directory_host_path.join(".tangram/socket");
		let proxy_server_socket_host_path =
			urlencoding::encode(proxy_server_socket_host_path.to_str().ok_or_else(
				|| tg::error!(%path = proxy_server_socket_host_path.display(), "invalid path"),
			)?);
		let proxy_server_host_url = format!("http+unix://{proxy_server_socket_host_path}");
		let proxy_server_host_url = Url::parse(&proxy_server_host_url)
			.map_err(|source| tg::error!(!source, "failed to parse the proxy server url"))?;

		// Start the proxy server.
		let proxy = Proxy::new(
			server.clone(),
			build.id().clone(),
			remote.clone(),
			Some(path_map),
		);
		let listener = Server::listen(&proxy_server_host_url).await?;
		let proxy_task = Task::spawn(|stop| Server::serve(proxy, listener, stop));

		// Render the executable.
		let Some(tg::target::Executable::Artifact(executable)) =
			target.executable(server).await?.as_ref().cloned()
		else {
			return Err(tg::error!("invalid executable"));
		};
		let executable =
			render(server, &executable.into(), &artifacts_directory_guest_path).await?;

		// Render the env.
		let env = target.env(server).await?;
		let mut env: BTreeMap<String, String> = env
			.iter()
			.map(|(key, value)| async {
				let key = key.clone();
				let value = render(server, value, &artifacts_directory_guest_path).await?;
				Ok::<_, tg::Error>((key, value))
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		// Render the args.
		let args = target.args(server).await?;
		let args: Vec<String> = args
			.iter()
			.map(|value| async {
				let value = render(server, value, &artifacts_directory_guest_path).await?;
				Ok::<_, tg::Error>(value)
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		// Enable the network if a checksum was provided.
		let network_enabled = checksum.is_some();

		// Set `$HOME`.
		env.insert(
			"HOME".to_owned(),
			home_directory_guest_path.to_str().unwrap().to_owned(),
		);

		// Set `$OUTPUT`.
		env.insert(
			"OUTPUT".to_owned(),
			output_guest_path.to_str().unwrap().to_owned(),
		);

		// Set `$TANGRAM_URL`.
		env.insert("TANGRAM_URL".to_owned(), proxy_server_guest_url.to_string());

		// Create /etc.
		tokio::fs::create_dir_all(root_directory_host_path.join("etc"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create /etc"))?;

		// Create /etc/passwd.
		tokio::fs::write(
			root_directory_host_path.join("etc/passwd"),
			formatdoc!(
				r#"
					root:!:0:0:root:/nonexistent:/bin/false
					tangram:!:{TANGRAM_UID}:{TANGRAM_GID}:tangram:{HOME_DIRECTORY_GUEST_PATH}:/bin/false
					nobody:!:65534:65534:nobody:/nonexistent:/bin/false
				"#
			),
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create /etc/passwd"))?;

		// Create /etc/group.
		tokio::fs::write(
			root_directory_host_path.join("etc/group"),
			formatdoc!(
				r#"
					tangram:x:{TANGRAM_GID}:tangram
				"#
			),
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create /etc/group"))?;

		// Create /etc/nsswitch.conf.
		tokio::fs::write(
			root_directory_host_path.join("etc/nsswitch.conf"),
			formatdoc!(
				r#"
					passwd: files compat
					shadow: files compat
					hosts: files dns compat
				"#
			),
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create /etc/nsswitch.conf"))?;

		// If network access is enabled, then copy /etc/resolv.conf from the host.
		if network_enabled {
			tokio::fs::copy(
				"/etc/resolv.conf",
				root_directory_host_path.join("etc/resolv.conf"),
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to copy /etc/resolv.conf"))?;
		}

		// Create the socket.
		let (mut host_socket, guest_socket) = tokio::net::UnixStream::pair()
			.map_err(|source| tg::error!(!source, "failed to create the socket pair"))?;
		let guest_socket = guest_socket
			.into_std()
			.map_err(|source| tg::error!(!source, "failed to convert the Unix Stream"))?;
		guest_socket
			.set_nonblocking(false)
			.map_err(|source| tg::error!(!source, "failed to set nonblocking mode"))?;

		// Create the mounts.
		let mut mounts = Vec::new();

		// Add /dev to the mounts.
		let dev_host_path = Path::new("/dev");
		let dev_guest_path = Path::new("/dev");
		let dev_source_path = dev_host_path;
		let dev_target_path =
			root_directory_host_path.join(dev_guest_path.strip_prefix("/").unwrap());
		tokio::fs::create_dir_all(&dev_target_path)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					r#"failed to create the mountpoint for "/dev""#
				)
			})?;
		let dev_source_path = CString::new(dev_source_path.as_os_str().as_bytes()).unwrap();
		let dev_target_path = CString::new(dev_target_path.as_os_str().as_bytes()).unwrap();
		mounts.push(Mount {
			source: dev_source_path,
			target: dev_target_path,
			fstype: None,
			flags: libc::MS_BIND | libc::MS_REC,
			data: None,
			readonly: false,
		});

		// Add /proc to the mounts.
		let proc_host_path = Path::new("/proc");
		let proc_guest_path = Path::new("/proc");
		let proc_source_path = proc_host_path;
		let proc_target_path =
			root_directory_host_path.join(proc_guest_path.strip_prefix("/").unwrap());
		tokio::fs::create_dir_all(&proc_target_path)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					r#"failed to create the mount point for "/proc""#
				)
			})?;
		let proc_source_path = CString::new(proc_source_path.as_os_str().as_bytes()).unwrap();
		let proc_target_path = CString::new(proc_target_path.as_os_str().as_bytes()).unwrap();
		mounts.push(Mount {
			source: proc_source_path,
			target: proc_target_path,
			fstype: Some(CString::new("proc").unwrap()),
			flags: 0,
			data: None,
			readonly: false,
		});

		// Add /tmp to the mounts.
		let tmp_host_path = Path::new("/tmp");
		let tmp_guest_path = Path::new("/tmp");
		let tmp_source_path = tmp_host_path;
		let tmp_target_path =
			root_directory_host_path.join(tmp_guest_path.strip_prefix("/").unwrap());
		tokio::fs::create_dir_all(&tmp_target_path)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					r#"failed to create the mount point for "/tmp""#
				)
			})?;
		let tmp_source_path = CString::new(tmp_source_path.as_os_str().as_bytes()).unwrap();
		let tmp_target_path = CString::new(tmp_target_path.as_os_str().as_bytes()).unwrap();
		mounts.push(Mount {
			source: tmp_source_path,
			target: tmp_target_path,
			fstype: Some(CString::new("tmpfs").unwrap()),
			flags: 0,
			data: None,
			readonly: false,
		});

		// Add the server directory to the mounts.
		let server_directory_source_path = &server_directory_host_path;
		let server_directory_target_path =
			root_directory_host_path.join(server_directory_guest_path.strip_prefix("/").unwrap());
		tokio::fs::create_dir_all(&server_directory_target_path)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					"failed to create the mount point for the tangram directory"
				)
			})?;
		let server_directory_source_path =
			CString::new(server_directory_source_path.as_os_str().as_bytes()).unwrap();
		let server_directory_target_path =
			CString::new(server_directory_target_path.as_os_str().as_bytes()).unwrap();
		mounts.push(Mount {
			source: server_directory_source_path,
			target: server_directory_target_path,
			fstype: None,
			flags: libc::MS_BIND | libc::MS_REC,
			data: None,
			readonly: false,
		});

		// Add the home directory to the mounts.
		let home_directory_source_path = home_directory_host_path.clone();
		let home_directory_target_path = home_directory_host_path.clone();
		let home_directory_source_path =
			CString::new(home_directory_source_path.as_os_str().as_bytes()).unwrap();
		let home_directory_target_path =
			CString::new(home_directory_target_path.as_os_str().as_bytes()).unwrap();
		mounts.push(Mount {
			source: home_directory_source_path,
			target: home_directory_target_path,
			fstype: None,
			flags: libc::MS_BIND | libc::MS_REC,
			data: None,
			readonly: false,
		});

		// Add the output parent directory to the mounts.
		let output_parent_directory_source_path = output_parent_directory_host_path.clone();
		let output_parent_directory_target_path = root_directory_host_path.join(
			output_parent_directory_guest_path
				.strip_prefix("/")
				.unwrap(),
		);
		tokio::fs::create_dir_all(&output_parent_directory_target_path)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					"failed to create the mount point for the output parent directory"
				)
			})?;
		let output_parent_directory_source_path =
			CString::new(output_parent_directory_source_path.as_os_str().as_bytes()).unwrap();
		let output_parent_directory_target_path =
			CString::new(output_parent_directory_target_path.as_os_str().as_bytes()).unwrap();
		mounts.push(Mount {
			source: output_parent_directory_source_path,
			target: output_parent_directory_target_path,
			fstype: None,
			flags: libc::MS_BIND | libc::MS_REC,
			data: None,
			readonly: false,
		});

		// Create the executable.
		let executable = CString::new(executable)
			.map_err(|source| tg::error!(!source, "the executable is not a valid C string"))?;

		// Create `envp`.
		let env = env
			.into_iter()
			.map(|(key, value)| format!("{key}={value}"))
			.map(|entry| CString::new(entry).unwrap())
			.collect_vec();
		let mut envp = Vec::with_capacity(env.len() + 1);
		for entry in env {
			envp.push(entry);
		}
		let envp = CStringVec::new(envp);

		// Create `argv`.
		let args: Vec<_> = args
			.into_iter()
			.map(CString::new)
			.try_collect()
			.map_err(|source| tg::error!(!source, "failed to convert the args"))?;
		let mut argv = Vec::with_capacity(1 + args.len() + 1);
		argv.push(executable.clone());
		for arg in args {
			argv.push(arg);
		}
		let argv = CStringVec::new(argv);

		// Get the root directory host path as a C string.
		let root_directory_host_path =
			CString::new(root_directory_host_path.as_os_str().as_bytes()).map_err(|error| {
				tg::error!(
					source = error,
					"the root directory host path is not a valid C string"
				)
			})?;

		// Get the working directory guest path as a C string.
		let working_directory_guest_path =
			CString::new(WORKING_DIRECTORY_GUEST_PATH).map_err(|error| {
				tg::error!(
					source = error,
					"the working directory is not a valid C string"
				)
			})?;

		// Create the log socket pair.
		let (log_send, log_recv) = tokio::net::UnixStream::pair()
			.map_err(|source| tg::error!(!source, "failed to create stdout socket"))?;
		let log = log_send
			.into_std()
			.map_err(|source| tg::error!(!source, "failed to convert the log sender"))?;
		log.set_nonblocking(false).map_err(|error| {
			tg::error!(
				source = error,
				"failed to set the log socket as non-blocking"
			)
		})?;

		// Create the context.
		let context = Context {
			argv,
			envp,
			executable,
			guest_socket,
			mounts,
			network_enabled,
			root_directory_host_path,
			working_directory_guest_path,
			log,
		};

		// Spawn the root process.
		let clone_flags = libc::CLONE_NEWUSER;
		let clone_flags = clone_flags
			.try_into()
			.map_err(|source| tg::error!(!source, "invalid clone flags"))?;
		let mut clone_args = libc::clone_args {
			flags: clone_flags,
			stack: 0,
			stack_size: 0,
			pidfd: 0,
			child_tid: 0,
			parent_tid: 0,
			exit_signal: 0,
			tls: 0,
			set_tid: 0,
			set_tid_size: 0,
			cgroup: 0,
		};
		let ret = unsafe {
			libc::syscall(
				libc::SYS_clone3,
				std::ptr::addr_of_mut!(clone_args),
				std::mem::size_of::<libc::clone_args>(),
			)
		};
		if ret == -1 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to spawn the root process"
			));
		}
		if ret == 0 {
			root(&context);
		}
		drop(context);
		let root_process_pid: libc::pid_t = ret
			.try_into()
			.map_err(|source| tg::error!(!source, "invalid root process PID"))?;

		// If this future is dropped, then kill the root process.
		scopeguard::defer! {
			// Kill the root process.
			let ret = unsafe { libc::kill(root_process_pid, libc::SIGKILL) };
			if ret != 0 {
				let error = std::io::Error::last_os_error();
				tracing::trace!(?ret, ?error, "failed to kill the root process");
				return;
			}

			// Wait for the root process to exit.
			tokio::task::spawn_blocking(move || {
				let mut status = 0;
				let ret = unsafe {
					libc::waitpid(
						root_process_pid,
						std::ptr::addr_of_mut!(status),
						libc::__WALL,
					)
				};
				if ret == -1 {
					let error = std::io::Error::last_os_error();
					tracing::trace!(?ret, ?error, "failed to wait for the root process to exit");
				}
			});
		};

		// Spawn the log task.
		let mut reader = log_recv;
		let log_task = tokio::task::spawn({
			let server = server.clone();
			let build = build.clone();
			let remote = remote.clone();
			async move {
				let mut buffer = vec![0; 4096];
				loop {
					let size = reader
						.read(&mut buffer)
						.await
						.map_err(|source| tg::error!(!source, "failed to read from the log"))?;
					if size == 0 {
						return Ok::<_, tg::Error>(());
					}
					let bytes = Bytes::copy_from_slice(&buffer[0..size]);
					if server.config.advanced.write_build_logs_to_stderr {
						tokio::io::stderr()
							.write_all(&bytes)
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to write the build log to stderr");
							})
							.ok();
					}
					let arg = tg::build::log::post::Arg {
						bytes,
						remote: remote.clone(),
					};
					build.add_log(&server, arg).await?;
				}
			}
		});

		// Receive the guest process's PID from the socket.
		let guest_process_pid: libc::pid_t = host_socket.read_i32_le().await.map_err(|error| {
			tg::error!(
				source = error,
				"failed to receive the PID of the guest process from the socket"
			)
		})?;

		// Write the guest process's UID map.
		let uid = unsafe { libc::getuid() };
		tokio::fs::write(
			format!("/proc/{guest_process_pid}/uid_map"),
			format!("{TANGRAM_UID} {uid} 1\n"),
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to set the UID map"))?;

		// Deny setgroups to the process.
		tokio::fs::write(format!("/proc/{guest_process_pid}/setgroups"), "deny")
			.await
			.map_err(|source| tg::error!(!source, "failed to disable setgroups"))?;

		// Write the guest process's GID map.
		let gid = unsafe { libc::getgid() };
		tokio::fs::write(
			format!("/proc/{guest_process_pid}/gid_map"),
			format!("{TANGRAM_GID} {gid} 1\n"),
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to set the GID map"))?;

		// Notify the guest process that it can continue.
		host_socket.write_u8(1).await.map_err(|error| {
			tg::error!(
				source = error,
				"failed to notify the guest process that it can continue"
			)
		})?;

		// Read the exit status from the host socket.
		let kind = host_socket.read_u8().await.map_err(|error| {
			tg::error!(
				source = error,
				"failed to receive the exit status kind from the root process"
			)
		})?;
		let value = host_socket.read_i32_le().await.map_err(|error| {
			tg::error!(
				source = error,
				"failed to receive the exit status value from the root process"
			)
		})?;
		let exit_status = match kind {
			0 => ExitStatus::Code(value),
			1 => ExitStatus::Signal(value),
			_ => unreachable!(),
		};

		// Wait for the root process to exit.
		tokio::task::spawn_blocking(move || {
			let mut status: libc::c_int = 0;
			let ret = unsafe {
				libc::waitpid(
					root_process_pid,
					std::ptr::addr_of_mut!(status),
					libc::__WALL,
				)
			};
			if ret == -1 {
				return Err(std::io::Error::last_os_error()).map_err(|error| {
					tg::error!(
						source = error,
						"failed to wait for the root process to exit"
					)
				});
			}
			let exit_status = if libc::WIFEXITED(status) {
				let status = libc::WEXITSTATUS(status);
				ExitStatus::Code(status)
			} else if libc::WIFSIGNALED(status) {
				let signal = libc::WTERMSIG(status);
				ExitStatus::Signal(signal)
			} else {
				unreachable!();
			};
			match exit_status {
				ExitStatus::Code(0) => (),
				ExitStatus::Code(code) => {
					return Err(tg::error!(r#"the root process exited with code "{code}""#));
				},
				ExitStatus::Signal(signal) => {
					return Err(tg::error!(
						r#"the root process was terminated with signal "{signal}""#
					));
				},
			};
			Ok(())
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the root process exit task"))?
		.map_err(|source| tg::error!(!source, "the root process did not exit successfully"))?;

		// Wait for the log task to complete.
		log_task
			.await
			.map_err(|source| tg::error!(!source, "failed to join the log task"))?
			.map_err(|source| tg::error!(!source, "the log task failed"))?;

		// Handle the guest process's exit status.
		match exit_status {
			ExitStatus::Code(0) => (),
			ExitStatus::Code(code) => {
				return Err(tg::error!(r#"the process exited with code "{code}""#));
			},
			ExitStatus::Signal(signal) => {
				let signame =
					unsafe { std::ffi::CStr::from_ptr(libc::strsignal(signal)).to_string_lossy() };
				return Err(tg::error!(
					r#"the process exited with signal: {signame} ({signal})"#
				));
			},
		};

		// Stop the proxy task.
		proxy_task.stop();
		proxy_task.wait().await.unwrap();

		// Create the output.
		let value = if tokio::fs::try_exists(&output_host_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to determine in the path exists"))?
		{
			let arg = tg::artifact::checkin::Arg {
				cache: true,
				destructive: true,
				deterministic: true,
				ignore: false,
				path: output_host_path.clone(),
				locked: true,
				lockfile: false,
			};
			tg::Artifact::check_in(server, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check in the output"))?
				.into()
		} else {
			tg::Value::Null
		};

		// Checksum the output if necessary.
		if let Some(checksum) = checksum.as_ref() {
			super::util::checksum(server, build, &value, checksum)
				.boxed()
				.await?;
		}

		Ok(value)
	}
}

/// Shared context between the host, root, and guest processes.
struct Context {
	/// The args.
	argv: CStringVec,

	/// The env.
	envp: CStringVec,

	/// The executable.
	executable: CString,

	/// The file descriptor of the guest side of the socket.
	guest_socket: std::os::unix::net::UnixStream,

	/// The mounts.
	mounts: Vec<Mount>,

	/// Whether to enable the network.
	network_enabled: bool,

	/// The host path to the root.
	root_directory_host_path: CString,

	/// The guest path to the working directory.
	working_directory_guest_path: CString,

	/// The file descriptor for streaming to the log.
	log: std::os::unix::net::UnixStream,
}

unsafe impl Send for Context {}

struct Mount {
	source: CString,
	target: CString,
	fstype: Option<CString>,
	flags: libc::c_ulong,
	data: Option<Vec<u8>>,
	readonly: bool,
}

fn root(context: &Context) {
	unsafe {
		// Ask to receive a SIGKILL signal if the host process exits.
		let ret = libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0);
		if ret == -1 {
			abort_errno!("failed to set PDEATHSIG");
		}

		// Duplicate stdout and stderr to the log.
		let ret = libc::dup2(context.log.as_raw_fd(), libc::STDOUT_FILENO);
		if ret == -1 {
			abort_errno!("failed to duplicate stdout to the log");
		}
		let ret = libc::dup2(context.log.as_raw_fd(), libc::STDERR_FILENO);
		if ret == -1 {
			abort_errno!("failed to duplicate stderr to the log");
		}

		// Close stdin.
		let ret = libc::close(libc::STDIN_FILENO);
		if ret == -1 {
			abort_errno!("failed to close stdin");
		}

		// If network access is disabled, set CLONE_NEWNET to isolate the guest's network namespace.
		let network_clone_flags = if context.network_enabled {
			0
		} else {
			libc::CLONE_NEWNET
		};

		// Spawn the guest process.
		let clone_flags = libc::CLONE_NEWNS | libc::CLONE_NEWPID | network_clone_flags;
		let Ok(clone_flags) = clone_flags.try_into() else {
			abort!("invalid clone flags");
		};
		let mut clone_args = libc::clone_args {
			flags: clone_flags,
			stack: 0,
			stack_size: 0,
			pidfd: 0,
			child_tid: 0,
			parent_tid: 0,
			exit_signal: 0,
			tls: 0,
			set_tid: 0,
			set_tid_size: 0,
			cgroup: 0,
		};
		let ret = libc::syscall(
			libc::SYS_clone3,
			std::ptr::addr_of_mut!(clone_args),
			std::mem::size_of::<libc::clone_args>(),
		);
		if ret == -1 {
			abort_errno!("failed to spawn the guest process");
		}
		if ret == 0 {
			guest(context);
		}
		let guest_process_pid: libc::pid_t = if let Ok(guest_process_pid) = ret.try_into() {
			guest_process_pid
		} else {
			abort!("invalid guest process PID");
		};

		// Send the guest process's PID to the host process, so the host process can write the UID and GID maps.
		let ret = libc::send(
			context.guest_socket.as_raw_fd(),
			std::ptr::addr_of!(guest_process_pid).cast(),
			std::mem::size_of_val(&guest_process_pid),
			0,
		);
		if ret == -1 {
			abort_errno!("failed to send the PID of guest process");
		}

		// Wait for the guest process.
		let mut status: libc::c_int = 0;
		let ret = libc::waitpid(guest_process_pid, &mut status, libc::__WALL);
		if ret == -1 {
			abort_errno!("failed to wait for the guest process");
		}
		let guest_process_exit_status = if libc::WIFEXITED(status) {
			let status = libc::WEXITSTATUS(status);
			ExitStatus::Code(status)
		} else if libc::WIFSIGNALED(status) {
			let signal = libc::WTERMSIG(status);
			ExitStatus::Signal(signal)
		} else {
			abort!("the guest process exited with neither a code nor a signal");
		};

		// Send the host process the exit code of the guest process.
		let (kind, value) = match guest_process_exit_status {
			ExitStatus::Code(code) => (0u8, code),
			ExitStatus::Signal(signal) => (1, signal),
		};
		let ret = libc::send(
			context.guest_socket.as_raw_fd(),
			std::ptr::addr_of!(kind).cast(),
			std::mem::size_of_val(&kind),
			0,
		);
		if ret == -1 {
			abort_errno!("failed to send the guest process's exit status's kind to the host");
		}
		let ret = libc::send(
			context.guest_socket.as_raw_fd(),
			std::ptr::addr_of!(value).cast(),
			std::mem::size_of_val(&value),
			0,
		);
		if ret == -1 {
			abort_errno!("failed to send the guest process's exit status's value to the host");
		}

		std::process::exit(0)
	}
}

fn guest(context: &Context) {
	unsafe {
		// Ask to receive a SIGKILL signal if the host process exits.
		let ret = libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0);
		if ret == -1 {
			abort_errno!("failed to set PDEATHSIG");
		}

		// Wait for the notification from the host process to continue.
		let mut notification = 0u8;
		let ret = libc::recv(
			context.guest_socket.as_raw_fd(),
			std::ptr::addr_of_mut!(notification).cast(),
			std::mem::size_of_val(&notification),
			0,
		);
		if ret == -1 {
			abort_errno!(
				"the guest process failed to receive the notification from the host process to continue"
			);
		}
		assert_eq!(notification, 1);

		// Perform the mounts.
		for mount in &context.mounts {
			let source = mount.source.as_ptr();
			let target = mount.target.as_ptr();
			let fstype = mount
				.fstype
				.as_ref()
				.map_or_else(std::ptr::null, |value| value.as_ptr());
			let flags = mount.flags;
			let data = mount
				.data
				.as_ref()
				.map_or_else(std::ptr::null, Vec::as_ptr)
				.cast();
			let ret = libc::mount(source, target, fstype, flags, data);
			if ret == -1 {
				abort_errno!(
					r#"failed to mount "{}" to "{}""#,
					mount.source.to_str().unwrap(),
					mount.target.to_str().unwrap(),
				);
			}
			if mount.readonly {
				let ret = libc::mount(
					source,
					target,
					fstype,
					flags | libc::MS_RDONLY | libc::MS_REMOUNT,
					data,
				);
				if ret == -1 {
					abort_errno!(
						r#"failed to mount "{}" to "{}""#,
						mount.source.to_str().unwrap(),
						mount.target.to_str().unwrap(),
					);
				}
			}
		}

		// Mount the root.
		let ret = libc::mount(
			context.root_directory_host_path.as_ptr(),
			context.root_directory_host_path.as_ptr(),
			std::ptr::null(),
			libc::MS_BIND | libc::MS_PRIVATE | libc::MS_REC,
			std::ptr::null(),
		);
		if ret == -1 {
			abort_errno!("failed to mount the root");
		}

		// Change the working directory to the pivoted root.
		let ret = libc::chdir(context.root_directory_host_path.as_ptr());
		if ret == -1 {
			abort_errno!("failed to change directory to the root");
		}

		// Pivot the root.
		let ret = libc::syscall(libc::SYS_pivot_root, c".".as_ptr(), c".".as_ptr());
		if ret == -1 {
			abort_errno!("failed to pivot the root");
		}

		// Unmount the root.
		let ret = libc::umount2(c".".as_ptr().cast(), libc::MNT_DETACH);
		if ret == -1 {
			abort_errno!("failed to unmount the root");
		}

		// Remount the root as read-only.
		let ret = libc::mount(
			std::ptr::null(),
			c"/".as_ptr().cast(),
			std::ptr::null(),
			libc::MS_BIND | libc::MS_PRIVATE | libc::MS_RDONLY | libc::MS_REC | libc::MS_REMOUNT,
			std::ptr::null(),
		);
		if ret == -1 {
			abort_errno!("failed to remount the root as read-only");
		}

		// Set the working directory.
		let ret = libc::chdir(context.working_directory_guest_path.as_ptr());
		if ret == -1 {
			abort_errno!("failed to set the working directory");
		}

		// Exec.
		libc::execve(
			context.executable.as_ptr(),
			context.argv.as_ptr().cast(),
			context.envp.as_ptr().cast(),
		);
		abort_errno!(r#"failed to call execve"#);
	}
}

struct CStringVec {
	_strings: Vec<CString>,
	pointers: Vec<*const libc::c_char>,
}

impl CStringVec {
	pub fn new(strings: Vec<CString>) -> Self {
		let mut pointers = strings.iter().map(|string| string.as_ptr()).collect_vec();
		pointers.push(std::ptr::null());
		Self {
			_strings: strings,
			pointers,
		}
	}

	pub fn as_ptr(&self) -> *const libc::c_char {
		self.pointers.as_ptr().cast()
	}
}

unsafe impl Send for CStringVec {}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ExitStatus {
	Code(i32),
	Signal(i32),
}

macro_rules! abort {
	($($t:tt)*) => {{
		eprintln!("an error occurred in the linux runtime guest process");
		eprintln!("{}", format_args!($($t)*));
		std::process::exit(1)
	}};
}

use abort;

macro_rules! abort_errno {
	($($t:tt)*) => {{
		eprintln!("an error occurred in the linux runtime guest process");
		eprintln!("{}", format_args!($($t)*));
		eprintln!("{}", std::io::Error::last_os_error());
		std::process::exit(1)
	}};
}

use abort_errno;
