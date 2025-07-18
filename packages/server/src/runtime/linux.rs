use super::{
	proxy::Proxy,
	util::{render_env, render_value, signal_task, stdio_task, which},
};
use crate::{Server, temp::Temp};
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::{
	collections::HashMap,
	os::unix::{ffi::OsStrExt as _, process::ExitStatusExt as _},
	path::{Path, PathBuf},
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::task::Task;
use url::Url;

#[derive(Clone)]
pub struct Runtime {
	pub(super) server: Server,
}

impl Runtime {
	pub async fn new(server: &Server) -> tg::Result<Self> {
		let server = server.clone();
		Ok(Self { server })
	}

	pub async fn run(&self, process: &tg::Process) -> super::Output {
		self.run_inner(process)
			.await
			.unwrap_or_else(|error| super::Output {
				checksum: None,
				error: Some(error),
				exit: 1,
				output: None,
			})
	}

	async fn run_inner(&self, process: &tg::Process) -> tg::Result<super::Output> {
		let sandbox = self
			.server
			.config()
			.runtimes
			.get("linux")
			.ok_or_else(|| tg::error!("server has no runtime configured for linux"))
			.cloned()?;
		if !matches!(sandbox.kind, crate::config::RuntimeKind::Tangram) {
			return Err(tg::error!("unsupported sandbox kind"));
		}
		let state = process.load(&self.server).await?;
		let command = process.command(&self.server).await?;
		let command = command.data(&self.server).await?;
		let remote = process.remote();

		// Checkout the process's chidlren.
		self.server.checkout_children(process).await?;

		// Determine if there is a root mount.
		let is_bind_mount_at_root = state
			.mounts
			.iter()
			.any(|mount| mount.source == mount.target && mount.target == Path::new("/"));

		// Create the temp.
		let temp = Temp::new(&self.server);
		let output_path = temp.path().join("output");
		let mut root_path = temp.path().join("root");

		// Create the output directory.
		tokio::fs::create_dir_all(&output_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the output directory"))?;

		// Create the command.
		let mut cmd = tokio::process::Command::new(sandbox.executable);
		cmd.args(sandbox.args);

		// Create mounts.
		let iter = std::iter::empty()
			.chain(state.mounts.iter().map(Either::Left))
			.chain(command.mounts.iter().map(Either::Right));
		let mut overlays = HashMap::new();
		for mount in iter {
			match mount {
				Either::Left(mount) => {
					cmd.arg("--mount");
					cmd.arg(bind(&mount.source, &mount.target, mount.readonly));
				},
				Either::Right(mount) => {
					// Create the overlay state if it does not exist. Since we use async here, we can't use the .entry() api.
					if !overlays.contains_key(&mount.target) {
						let lowerdirs = Vec::new();
						let upperdir = temp.path().join("upper").join(overlays.len().to_string());
						let workdir = temp.path().join("work").join(overlays.len().to_string());
						tokio::fs::create_dir_all(&upperdir).await.ok();
						tokio::fs::create_dir_all(&workdir).await.ok();
						if mount.target == Path::new("/") {
							root_path = upperdir.clone();
						}
						overlays.insert(mount.target.clone(), (lowerdirs, upperdir, workdir));
					}

					// Compute the path.
					let path = self.server.artifacts_path().join(mount.source.to_string());

					// Get the lower dirs.
					let (lowerdirs, _, _) = overlays.get_mut(&mount.target).unwrap();

					// Add this path to the lowerdirs.
					lowerdirs.push(path);
				},
			}
		}

		// Create the proxy path.
		let path = temp.path().join(".tangram");
		tokio::fs::create_dir_all(&path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to create the data directory"),
		)?;

		// Add additional mounts.
		if !is_bind_mount_at_root {
			// Create /etc.
			tokio::fs::create_dir_all(temp.path().join("lower/etc"))
				.await
				.ok();

			// Create nsswitch.conf.
			tokio::fs::write(
				temp.path().join("lower/etc/nsswitch.conf"),
				formatdoc!(
					"
						passwd: files compat
						shadow: files compat
						hosts: files dns compat
					"
				),
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to create /etc/nsswitch.conf"))?;

			// Create /etc/passwd.
			tokio::fs::write(
				temp.path().join("lower/etc/passwd"),
				formatdoc!(
					"
						root:!:0:0:root:/nonexistent:/bin/false
						nobody:!:65534:65534:nobody:/nonexistent:/bin/false
					"
				),
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to create /etc/passwd"))?;

			// Copy resolv.conf.
			if state.network {
				tokio::fs::copy(
					"/etc/resolv.conf",
					temp.path().join("lower/etc/resolv.conf"),
				)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to copy /etc/resolv.conf to sandbox")
				})?;
				cmd.arg("--network");
			}

			// Get or create the root overlay.
			if !overlays.contains_key(Path::new("/")) {
				let lowerdirs = Vec::new();
				let upperdir = temp.path().join("upper").join(overlays.len().to_string());
				let workdir = temp.path().join("work").join(overlays.len().to_string());
				tokio::fs::create_dir_all(&upperdir).await.ok();
				tokio::fs::create_dir_all(&workdir).await.ok();
				root_path = upperdir.clone();
				overlays.insert("/".into(), (lowerdirs, upperdir, workdir));
			}
			let (lowerdirs, _, _) = overlays.get_mut(Path::new("/")).unwrap();
			lowerdirs.push(temp.path().join("lower"));

			// Add mounts for /dev, /proc, /tmp, /.tangram, /.tangram/artifacts, and /output.
			cmd.arg("--mount")
				.arg(bind("/dev", "/dev", false))
				.arg("--mount")
				.arg(bind("/proc", "/proc", false))
				.arg("--mount")
				.arg("type=tmpfs,source=/tmp,target=/tmp")
				.arg("--mount")
				.arg(bind(temp.path().join(".tangram"), "/.tangram", false))
				.arg("--mount")
				.arg(bind(
					self.server.artifacts_path(),
					"/.tangram/artifacts",
					false,
				))
				.arg("--mount")
				.arg(bind(&output_path, "/output", false));
		}

		// Add the overlay mounts.
		for (merged, (lowerdirs, upperdir, workdir)) in &overlays {
			cmd.arg("--mount")
				.arg(overlay(lowerdirs, upperdir, workdir, merged));
		}

		// Get the artifacts path.
		let artifacts_path = if is_bind_mount_at_root {
			self.server.artifacts_path()
		} else {
			"/.tangram/artifacts".into()
		};

		// Create the proxy server.
		let proxy = if is_bind_mount_at_root {
			None
		} else {
			// Create the path map.
			let path_map = crate::runtime::proxy::PathMap {
				output_host: output_path.clone(),
				output_guest: "/output".into(),
				root_host: root_path.clone(),
			};

			// Create the proxy server guest URL.
			let socket = Path::new("/.tangram/socket");
			let socket = urlencoding::encode(
				socket
					.to_str()
					.ok_or_else(|| tg::error!(%path = socket.display(), "invalid path"))?,
			);
			let guest_url = format!("http+unix://{socket}").parse::<Url>().unwrap();

			// Create the proxy server host URL.
			let socket = temp.path().join(".tangram/socket");
			let socket = urlencoding::encode(
				socket
					.to_str()
					.ok_or_else(|| tg::error!(%path = socket.display(), "invalid path"))?,
			);
			let host_url = format!("http+unix://{socket}").parse::<Url>().unwrap();

			// Start the proxy server.
			let proxy = Proxy::new(
				self.server.clone(),
				process,
				remote.cloned(),
				Some(path_map),
			);
			let listener = Server::listen(&host_url).await?;
			let task = Task::spawn(|stop| Server::serve(proxy, listener, stop));
			Some((task, guest_url))
		};

		// Render the args.
		let args = command
			.args
			.iter()
			.map(|value| render_value(&artifacts_path, value));

		// Get the working directory.
		let cwd = if let Some(cwd) = &command.cwd {
			cwd.clone()
		} else {
			"/".into()
		};
		cmd.arg("-C").arg(cwd);

		// Render the env.
		let env = render_env(&artifacts_path, &command.env)?;

		// Add env vars to the command.
		for (name, value) in &env {
			cmd.arg("-e").arg(format!("{name}={value}"));
		}

		// Render the executable.
		let executable = match command.executable {
			tg::command::data::Executable::Artifact(executable) => {
				let mut path = artifacts_path.join(executable.artifact.to_string());
				if let Some(subpath) = executable.path {
					path = path.join(subpath);
				}
				path
			},
			tg::command::data::Executable::Module(_) => {
				return Err(tg::error!("invalid executable"));
			},
			tg::command::data::Executable::Path(executable) => {
				which(&executable.path, &env).await?
			},
		};

		// Set `$OUTPUT`.
		if is_bind_mount_at_root {
			cmd.arg("-e")
				.arg(format!("OUTPUT={}/output", output_path.display()));
		} else {
			cmd.arg("-e").arg("OUTPUT=/output/output");
		}

		// Set `$TANGRAM_PROCESS`.
		cmd.arg("-e")
			.arg(format!("TANGRAM_PROCESS={}", process.id()));

		// Set `$TANGRAM_URL`.
		let url = proxy.as_ref().map_or_else(
			|| {
				let path = self.server.path.join("socket").display().to_string();
				let path = urlencoding::encode(&path);
				format!("http+unix://{path}")
			},
			|(_, url)| url.to_string(),
		);
		cmd.arg("-e").arg(format!("TANGRAM_URL={url}"));

		// Create the stdio.
		match state.stdin.as_ref() {
			_ if command.stdin.is_some() => {
				cmd.stdin(std::process::Stdio::piped());
			},
			Some(tg::process::Stdio::Pipe(pipe)) => {
				let fd = self.server.get_pipe_fd(pipe, true)?;
				cmd.stdin(fd);
			},
			Some(tg::process::Stdio::Pty(pty)) => {
				let fd = self.server.get_pty_fd(pty, false)?;
				cmd.stdin(fd);
			},
			None => {
				cmd.stdin(std::process::Stdio::null());
			},
		}
		match state.stdout.as_ref() {
			Some(tg::process::Stdio::Pipe(pipe)) => {
				let fd = self.server.get_pipe_fd(pipe, false)?;
				cmd.stdout(fd);
			},
			Some(tg::process::Stdio::Pty(pty)) => {
				let fd = self.server.get_pty_fd(pty, false)?;
				cmd.stdout(fd);
			},
			None => {
				cmd.stdout(std::process::Stdio::piped());
			},
		}
		match state.stdout.as_ref() {
			Some(tg::process::Stdio::Pipe(pipe)) => {
				let fd = self.server.get_pipe_fd(pipe, false)?;
				cmd.stderr(fd);
			},
			Some(tg::process::Stdio::Pty(pty)) => {
				let fd = self.server.get_pty_fd(pty, false)?;
				cmd.stderr(fd);
			},
			None => {
				cmd.stderr(std::process::Stdio::piped());
			},
		}

		// Set the user.
		let user = command.user.unwrap_or_else(|| "root".into());
		cmd.arg("--user").arg(user);

		// Set the hostname
		cmd.arg("--hostname").arg(process.id().to_string());

		// Set the chroot.
		cmd.arg("--chroot").arg(root_path);

		// Spawn the process.
		let mut child = cmd
			.arg(executable)
			.arg("--")
			.args(args)
			.current_dir("/")
			.env_clear()
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the sandbox process"))?;

		// Spawn the stdio task.
		let stdio_task = tokio::spawn({
			let server = self.server.clone();
			let process = process.clone();

			// Get the stdio.
			let stdin = child.stdin.take();
			let stdout = child.stdout.take();
			let stderr = child.stderr.take();
			async move { stdio_task(&server, &process, stdin, stdout, stderr).await }
		});

		// Spawn the signal task.
		let signal_task = tokio::spawn({
			let server = self.server.clone();
			let process = process.clone();
			let pid = child.id().unwrap().to_i32().unwrap();
			async move {
				signal_task(&server, pid, &process)
					.await
					.inspect_err(|source| tracing::error!(?source, "signal task failed"))
					.ok();
			}
		});

		// Wait for the process to complete.
		let exit = child.wait().await.map_err(
			|source| tg::error!(!source, %process = process.id(), "failed to wait for the child process"),
		)?;
		let exit = exit.code().or(exit.signal()).unwrap().to_u8().unwrap();

		// Stop and await the proxy task.
		if let Some(task) = proxy.map(|(proxy, _)| proxy) {
			task.stop();
			task.wait().await.unwrap();
		}

		// Abort the signal task.
		signal_task.abort();

		// Stop and await the stdio task.
		stdio_task.await.unwrap()?;

		// Create the output.
		let output = output_path.join("output");
		let exists = tokio::fs::try_exists(&output).await.map_err(|source| {
			tg::error!(!source, "failed to determine if the output path exists")
		})?;
		let output = if exists {
			let arg = tg::checkin::Arg {
				destructive: true,
				deterministic: true,
				ignore: false,
				path: output.clone(),
				lock: false,
				locked: true,
				updates: Vec::new(),
			};
			let artifact = tg::checkin(&self.server, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check in the output"))?;
			Some(tg::Value::from(artifact))
		} else {
			None
		};

		// Create the output.
		let output = super::Output {
			checksum: None,
			error: None,
			exit,
			output,
		};

		Ok(output)
	}
}

fn bind(source: impl AsRef<Path>, target: impl AsRef<Path>, readonly: bool) -> String {
	let mut string = format!(
		"type=bind,source={},target={}",
		source.as_ref().display(),
		target.as_ref().display()
	);
	if readonly {
		string.push_str(",ro");
	}
	string
}

fn overlay(lowerdirs: &[PathBuf], upperdir: &Path, workdir: &Path, merged: &Path) -> String {
	fn escape(out: &mut Vec<u8>, path: &[u8]) {
		for byte in path.iter().copied() {
			if byte == 0 {
				break;
			}
			if byte == b':' {
				out.push(b'\\');
			}
			out.push(byte);
		}
	}

	// Create the mount options.
	let mut data = b"type=overlay,source=overlay,target=".to_vec();
	data.extend_from_slice(merged.as_os_str().as_bytes());

	// Add the lower directories.
	data.extend_from_slice(b",userxattr,lowerdir=");
	for (n, dir) in lowerdirs.iter().enumerate() {
		escape(&mut data, dir.as_os_str().as_bytes());
		if n != lowerdirs.len() - 1 {
			data.push(b':');
		}
	}

	// Add the upper directory.
	data.extend_from_slice(b",upperdir=");
	data.extend_from_slice(upperdir.as_os_str().as_bytes());

	// Add the working directory.
	data.extend_from_slice(b",workdir=");
	data.extend_from_slice(workdir.as_os_str().as_bytes());

	String::from_utf8(data).unwrap()
}
