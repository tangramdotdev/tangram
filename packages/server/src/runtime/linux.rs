use {
	super::{
		proxy::Proxy,
		util::{render_env, render_value, signal_task, stdio_task, which},
	},
	crate::{Server, temp::Temp},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{
		collections::{BTreeMap, HashMap},
		os::{
			fd::IntoRawFd as _,
			unix::{ffi::OsStrExt as _, process::ExitStatusExt as _},
		},
		path::{Path, PathBuf},
	},
	tangram_client as tg,
	tangram_either::Either,
	tangram_futures::task::Task,
	tangram_uri::Uri,
};

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
		let config = self
			.server
			.config()
			.runtimes
			.get("linux")
			.ok_or_else(|| tg::error!("server has no runtime configured for linux"))
			.cloned()?;
		if !matches!(config.kind, crate::config::RuntimeKind::Tangram) {
			return Err(tg::error!("unsupported runtime kind"));
		}

		let state = process.load(&self.server).await?;
		let command = process.command(&self.server).await?;
		let command = command.data(&self.server).await?;

		// Checkout the process's chidlren.
		self.server.checkout_children(process).await?;

		// Create the temp.
		let temp = Temp::new(&self.server);
		// Create the output directory.
		tokio::fs::create_dir_all(temp.path().join("output"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the temp directory"))?;

		// Determine if there is a root mount.
		let is_bind_mount_at_root = state
			.mounts
			.iter()
			.any(|mount| mount.source == mount.target && mount.target == Path::new("/"));

		// Get the artifacts path.
		let artifacts_path = if is_bind_mount_at_root {
			self.server.artifacts_path()
		} else {
			"/.tangram/artifacts".into()
		};

		// Render the args.
		let args = command
			.args
			.iter()
			.map(|value| render_value(&artifacts_path, value))
			.collect::<Vec<_>>();

		// Get the working directory.
		let cwd = if let Some(cwd) = &command.cwd {
			cwd.clone()
		} else {
			"/".into()
		};

		// Render the env.
		let mut env = render_env(&artifacts_path, &command.env)?;

		// Render the executable.
		let executable = match &command.executable {
			tg::command::data::Executable::Artifact(executable) => {
				let mut path = artifacts_path.join(executable.artifact.to_string());
				if let Some(executable_path) = &executable.path {
					path = path.join(executable_path);
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

		// Create mounts.
		let mounts = std::iter::empty()
			.chain(state.mounts.iter().map(Either::Left))
			.chain(command.mounts.iter().map(Either::Right))
			.collect::<Vec<_>>();

		// Determine whether to use the sandbox.
		let sandbox = !(is_bind_mount_at_root
			&& (mounts.len() == 1)
			&& state.network
			&& command.user.as_ref().is_none_or(|usr| {
				let Ok(username) = crate::runtime::util::whoami()
					.inspect_err(|error| tracing::error!(?error, "failed to get username"))
				else {
					return false;
				};
				usr == &username
			}));

		// Create the proxy server.
		let proxy = if is_bind_mount_at_root {
			None
		} else {
			// Create the path map.
			let path_map = crate::runtime::proxy::PathMap {
				output_host: temp.path().join("output"),
				output_guest: "/output".into(),
				root_host: temp.path().join("root"),
			};

			// Create the proxy server guest URL.
			let socket = Path::new("/.tangram/socket");
			let socket = socket
				.to_str()
				.ok_or_else(|| tg::error!(%path = socket.display(), "invalid path"))?;
			let socket = urlencoding::encode(socket);
			let guest_url = format!("http+unix://{socket}").parse::<Uri>().unwrap();

			// Create the proxy server host URL.
			let socket = temp.path().join(".tangram/socket");
			tokio::fs::create_dir_all(socket.parent().unwrap())
				.await
				.map_err(|source| tg::error!(!source, "failed to create the host path"))?;

			let socket = urlencoding::encode(
				socket
					.to_str()
					.ok_or_else(|| tg::error!(%path = socket.display(), "invalid path"))?,
			);
			let host_url = format!("http+unix://{socket}").parse::<Uri>().unwrap();

			// Start the proxy server.
			let proxy = Proxy::new(
				self.server.clone(),
				process,
				process.remote().cloned(),
				Some(path_map),
			);
			let listener = Server::listen(&host_url).await?;
			let task = Task::spawn(|stop| Server::serve(proxy, listener, stop));
			Some((task, guest_url))
		};

		// Add our env vars.
		env.insert("TANGRAM_PROCESS".to_owned(), process.id().to_string());

		let url = proxy.as_ref().map_or_else(
			|| {
				let path = self.server.path.join("socket").display().to_string();
				let path = urlencoding::encode(&path);
				format!("http+unix://{path}")
			},
			|(_, url)| url.to_string(),
		);
		env.insert("TANGRAM_URL".to_owned(), url);

		if is_bind_mount_at_root {
			env.insert(
				"OUTPUT".to_owned(),
				format!("{}/output/output", temp.path().display()),
			);
		} else {
			env.insert("OUTPUT".to_owned(), "/output/output".to_owned());
		}

		// Create the command.
		let mut cmd = if sandbox {
			self.sandbox_command(
				&config,
				&args,
				&cwd,
				&env,
				&executable,
				&mounts,
				is_bind_mount_at_root,
				&temp,
				state.as_ref(),
				process,
				&command,
			)
			.await?
		} else {
			let mut cmd = tokio::process::Command::new(executable);
			cmd.args(args).env_clear().current_dir(cwd).envs(env.iter());
			cmd
		};

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
				let host = self.server.get_pty_fd(pty, true)?.into_raw_fd();
				let guest = self.server.get_pty_fd(pty, false)?;
				cmd.stdin(guest);
				unsafe {
					cmd.pre_exec(move || {
						libc::close(host);
						Ok(())
					});
				}
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
		match state.stderr.as_ref() {
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

		// Spawn the process.
		let mut child = cmd
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

		// Spawn the stdio task.
		let stdio_task = tokio::spawn({
			let server = self.server.clone();
			let process = process.clone();
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
		let exit = exit
			.code()
			.or(exit.signal().map(|signal| 128 + signal))
			.unwrap()
			.to_u8()
			.unwrap();

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
		let output = temp.path().join("output/output");
		let exists = tokio::fs::try_exists(&output).await.map_err(|source| {
			tg::error!(!source, "failed to determine if the output path exists")
		})?;
		let output = if exists {
			let arg = tg::checkin::Arg {
				options: tg::checkin::Options {
					destructive: true,
					deterministic: true,
					ignore: false,
					lock: false,
					locked: true,
					..Default::default()
				},
				path: output.clone(),
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

	#[allow(clippy::too_many_arguments)]
	async fn sandbox_command(
		&self,
		config: &crate::config::Runtime,
		args: &[String],
		cwd: &Path,
		env: &BTreeMap<String, String>,
		executable: &Path,
		mounts: &[Either<&tg::process::Mount, &tg::command::data::Mount>],
		is_bind_mount_at_root: bool,
		temp: &Temp,
		state: &tg::process::State,
		process: &tg::Process,
		command: &tg::command::Data,
	) -> tg::Result<tokio::process::Command> {
		// Create the output/root paths.
		let output_path = temp.path().join("output");
		let mut root_path = temp.path().join("root");

		// Create the command.
		let mut cmd = tokio::process::Command::new(&config.executable);
		cmd.args(&config.args);

		let mut overlays = HashMap::new();
		for mount in mounts {
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

		// Add additional mounts.
		if !is_bind_mount_at_root {
			// Create the proxy path.
			let path = temp.path().join(".tangram");
			tokio::fs::create_dir_all(&path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to create the data directory"),
			)?;

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

		cmd.arg("-C").arg(cwd);

		// Add env vars to the command.
		for (name, value) in env {
			cmd.arg("-e").arg(format!("{name}={value}"));
		}

		// Set the user.
		let user = command.user.as_deref().unwrap_or("root");
		cmd.arg("--user").arg(user);

		// Set the hostname
		cmd.arg("--hostname").arg(process.id().to_string());

		// Set the chroot.
		cmd.arg("--chroot").arg(root_path);

		// Finish the command setup.
		cmd.arg(executable)
			.arg("--")
			.args(args)
			.current_dir("/")
			.env_clear();
		Ok(cmd)
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
