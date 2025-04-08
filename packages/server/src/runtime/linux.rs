use super::{
	proxy::Proxy,
	util::{render_env, render_value, signal_task, stdio_task, which},
};
use crate::{Server, temp::Temp};
use futures::stream::{FuturesUnordered, TryStreamExt as _};
use indoc::formatdoc;
use std::path::Path;
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::task::Task;
use tangram_sandbox as sandbox;
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
		let (error, exit, output) = match self.run_inner(process).await {
			Ok((exit, output)) => (None, exit, output),
			Err(error) => (Some(error), None, None),
		};
		super::Output {
			error,
			exit,
			output,
		}
	}

	async fn run_inner(
		&self,
		process: &tg::Process,
	) -> tg::Result<(Option<tg::process::Exit>, Option<tg::Value>)> {
		let state = process.load(&self.server).await?;
		let command = process.command(&self.server).await?;
		let command = command.data(&self.server).await?;
		let remote = process.remote();

		// If the VFS is disabled, then check out the target's children.
		if self.server.vfs.lock().unwrap().is_none() {
			command
				.children()
				.into_iter()
				.filter_map(|id| Some(tg::Artifact::with_id(id.try_into().ok()?)))
				.map(|artifact| async move {
					let arg = tg::artifact::checkout::Arg::default();
					artifact.check_out(&self.server, arg).await?;
					Ok::<_, tg::Error>(())
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await?;
		}

		// Determine if there is a root mount.
		let root_bind_mount = state
			.mounts
			.iter()
			.any(|mount| mount.source == mount.target && mount.target == Path::new("/"));

		// Create the temp.
		let temp = Temp::new(&self.server);
		let output_path = temp.path().join("output");
		let mut root_path = temp.path().join("root");
		let mut mounts = Vec::with_capacity(state.mounts.len() + command.mounts.len());

		// Create the output directory.
		tokio::fs::create_dir_all(&output_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the output directory"))?;

		// Create mounts.
		let mut overlays: Vec<sandbox::Overlay> = vec![];
		let iter = state
			.mounts
			.iter()
			.map(Either::Left)
			.chain(command.mounts.iter().map(Either::Right));
		for mount in iter {
			match mount {
				Either::Left(mount) => {
					let mount = sandbox::BindMount {
						source: mount.source.clone(),
						target: mount.target.clone(),
						readonly: mount.readonly,
					};
					mounts.push(mount.into());
				},
				Either::Right(mount) => {
					let overlay = 'a: {
						if let Some(overlay) = overlays
							.iter_mut()
							.find(|overlay| overlay.merged == mount.target)
						{
							break 'a overlay;
						};
						let upperdir = temp.path().join("upper").join(overlays.len().to_string());
						let workdir = temp.path().join("work").join(overlays.len().to_string());
						tokio::fs::create_dir_all(&upperdir).await.ok();
						tokio::fs::create_dir_all(&workdir).await.ok();
						if mount.target == Path::new("/") {
							root_path = upperdir.clone();
						}
						overlays.push(sandbox::Overlay {
							lowerdirs: vec![],
							upperdir,
							workdir,
							merged: mount.target.clone(),
						});
						overlays.last_mut().unwrap()
					};
					let path = self.server.artifacts_path().join(mount.source.to_string());
					overlay.lowerdirs.push(path);
				},
			}
		}

		// Create the proxy path.
		let path = temp.path().join(".tangram");
		tokio::fs::create_dir_all(&path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to create the data directory"),
		)?;

		// Add additional mounts.
		if !root_bind_mount {
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
			}

			let overlay = 'a: {
				if let Some(overlay) = overlays
					.iter_mut()
					.find(|overlay| overlay.merged == Path::new("/"))
				{
					break 'a overlay;
				};
				let upperdir = temp.path().join("upper").join(overlays.len().to_string());
				let workdir = temp.path().join("work").join(overlays.len().to_string());
				tokio::fs::create_dir_all(&upperdir).await.ok();
				tokio::fs::create_dir_all(&workdir).await.ok();
				root_path = upperdir.clone();
				overlays.push(sandbox::Overlay {
					lowerdirs: vec![],
					upperdir,
					workdir,
					merged: "/".into(),
				});
				overlays.last_mut().unwrap()
			};
			overlay.lowerdirs.push(temp.path().join("lower"));

			// Add mounts for /dev, /proc, /tmp, /.tangram, /.tangram/artifacts, and /output.
			mounts.push(
				sandbox::BindMount {
					source: "/dev".into(),
					target: "/dev".into(),
					readonly: false,
				}
				.into(),
			);
			mounts.push(sandbox::Mount {
				source: "/proc".into(),
				target: "/proc".into(),
				fstype: Some("proc".into()),
				flags: 0,
				data: None,
				readonly: false,
			});
			mounts.push(sandbox::Mount {
				source: "/tmp".into(),
				target: "/tmp".into(),
				fstype: Some("tmpfs".into()),
				flags: 0,
				data: None,
				readonly: false,
			});
			mounts.push(
				sandbox::BindMount {
					source: temp.path().join(".tangram"),
					target: "/.tangram".into(),
					readonly: false,
				}
				.into(),
			);
			mounts.push(
				sandbox::BindMount {
					source: self.server.artifacts_path(),
					target: "/.tangram/artifacts".into(),
					readonly: false,
				}
				.into(),
			);
			mounts.push(
				sandbox::BindMount {
					source: output_path.clone(),
					target: "/output".into(),
					readonly: false,
				}
				.into(),
			);
		}
		mounts.extend(overlays.into_iter().map(sandbox::Mount::from));
		mounts.sort_unstable_by_key(|mount| mount.target.components().count());

		// Get the artifacts path.
		let artifacts_path = if root_bind_mount {
			self.server.artifacts_path()
		} else {
			"/.tangram/artifacts".into()
		};

		// Create the proxy server.
		let proxy = if root_bind_mount {
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
		let args: Vec<String> = command
			.args
			.iter()
			.map(|value| render_value(&artifacts_path, value))
			.collect();

		// Get the working directory.
		let cwd = if let Some(cwd) = &command.cwd {
			cwd.clone()
		} else {
			"/".into()
		};

		// Render the env.
		let mut env = render_env(&artifacts_path, &command.env)?;

		// Render the executable.
		let executable = match command.executable {
			tg::command::data::Executable::Artifact(artifact) => {
				render_value(&artifacts_path, &tg::object::Id::from(artifact).into())
			},
			tg::command::data::Executable::Module(_) => {
				return Err(tg::error!("invalid executable"));
			},
			tg::command::data::Executable::Path(path) => which(&path, &env).await?,
		};

		// Set `$OUTPUT`.
		if root_bind_mount {
			env.insert(
				"OUTPUT".to_owned(),
				output_path.join("output").to_str().unwrap().to_owned(),
			);
		} else {
			env.insert("OUTPUT".to_owned(), "/output/output".to_owned());
		}

		// Set `$TANGRAM_PROCESS`.
		env.insert("TANGRAM_PROCESS".to_owned(), process.id().to_string());

		// Set `$TANGRAM_URL`.
		let url = proxy.as_ref().map_or_else(
			|| {
				let path = self.server.path.join("socket").display().to_string();
				let path = urlencoding::encode(&path);
				format!("http+unix://{path}")
			},
			|(_, url)| url.to_string(),
		);
		env.insert("TANGRAM_URL".to_owned(), url);

		// Create the stdio.
		let stdin = if let Some(tg::process::Stdio::Pty(pty)) = &state.stdin {
			let arg = tg::pty::read::Arg {
				remote: process.remote().cloned(),
				master: true,
			};
			let size = self
				.server
				.get_pty_size(pty, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to get the pty size"))?;
			let tty = sandbox::Tty {
				rows: size.rows,
				cols: size.cols,
			};
			sandbox::Stdio::Tty(tty)
		} else {
			sandbox::Stdio::Pipe
		};
		let stdout = if let Some(tg::process::Stdio::Pty(pty)) = &state.stdout {
			let arg = tg::pty::read::Arg {
				remote: process.remote().cloned(),
				master: false,
			};
			let size = self
				.server
				.get_pty_size(pty, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to get the pty size"))?;
			let tty = sandbox::Tty {
				rows: size.rows,
				cols: size.cols,
			};
			sandbox::Stdio::Tty(tty)
		} else {
			sandbox::Stdio::Pipe
		};
		let stderr = if let Some(tg::process::Stdio::Pty(pty)) = &state.stderr {
			let arg = tg::pty::read::Arg {
				remote: process.remote().cloned(),
				master: false,
			};
			let size = self
				.server
				.get_pty_size(pty, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to get the pty size"))?;
			let tty = sandbox::Tty {
				rows: size.rows,
				cols: size.cols,
			};
			sandbox::Stdio::Tty(tty)
		} else {
			sandbox::Stdio::Pipe
		};
		let user = command.user.unwrap_or_else(|| "root".into());

		// Spawn the process.
		let mut child = sandbox::Command::new(executable)
			.args(args)
			.chroot(root_path)
			.cwd(cwd)
			.envs(env)
			.hostname(process.id().to_string())
			.mounts(mounts)
			.stderr(stderr)
			.stdin(stdin)
			.stdout(stdout)
			.user(user)
			.map_err(|source| tg::error!(!source, "invalid user"))?
			.spawn()
			.await
			.map_err(|source| {
				if source.raw_os_error() == Some(libc::ENOSYS) {
					tg::error!(%errno = libc::ENOSYS, "cannot spawn within a container runtime without --privileged")
				} else {
					tg::error!(!source, "failed to spawn the child process")
				}
			})?;

		// Spawn the stdio task.
		let stdio_task = Task::spawn(|stop| {
			let server = self.server.clone();
			let process = process.clone();
			let stdin = child.stdin.take();
			let stdout = child.stdout.take();
			let stderr = child.stderr.take();
			async move { stdio_task(&server, &process, stop, stdin, stdout, stderr).await }
		});

		// Spawn the signal task.
		let signal_task = tokio::spawn({
			let server = self.server.clone();
			let process = process.clone();
			let pid = child.pid();
			async move {
				signal_task(&server, pid, &process)
					.await
					.inspect_err(|source| tracing::error!(?source, "signal task failed"))
					.ok();
			}
		});

		// Wait for the child process to complete.
		let exit = child.wait().await.map_err(
			|source| tg::error!(!source, %process = process.id(), "failed to wait for the child process"),
		)?;
		let exit = match exit {
			sandbox::ExitStatus::Code(code) => tg::process::Exit::Code { code },
			sandbox::ExitStatus::Signal(signal) => tg::process::Exit::Signal { signal },
		};

		// Stop and await the proxy task.
		if let Some(task) = proxy.map(|(proxy, _)| proxy) {
			task.stop();
			task.wait().await.unwrap();
		}

		// Abort the signal task.
		signal_task.abort();

		// Stop and await the stdio task.
		stdio_task.stop();
		stdio_task.wait().await.unwrap()?;

		// Create the output.
		let output = output_path.join("output");
		let exists = tokio::fs::try_exists(&output)
			.await
			.map_err(|source| tg::error!(!source, "failed to determine in the path exists"))?;
		let output = if exists {
			let arg = tg::artifact::checkin::Arg {
				cache: true,
				destructive: true,
				deterministic: true,
				ignore: false,
				path: output.clone(),
				locked: true,
				lockfile: false,
			};
			let artifact = tg::Artifact::check_in(&self.server, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check in the output"))?;
			Some(tg::Value::from(artifact))
		} else {
			None
		};

		Ok((Some(exit), output))
	}
}
