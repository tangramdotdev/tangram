use super::{
	proxy::{self, Proxy},
	util,
};
use crate::{Server, temp::Temp};
use futures::stream::{FuturesOrdered, FuturesUnordered, TryStreamExt as _};
use indoc::formatdoc;
use std::path::{Path, PathBuf};
use tangram_client as tg;
use tangram_futures::task::Task;
use tangram_sandbox as sandbox;
use url::Url;

#[derive(Clone)]
pub struct Runtime {
	pub(super) server: Server,
}

pub struct Instance {
	root_is_bind_mounted: bool,
	temp: Temp,
	mounts: Vec<sandbox::Mount>,
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
		// Get the process state.
		let state = process.load(&self.server).await?;
		let command = process.command(&self.server).await?;
		let remote = process.remote();

		// If the VFS is disabled, then check out the target's children.
		if self.server.vfs.lock().unwrap().is_none() {
			command
				.data(&self.server)
				.await?
				.children()
				.into_iter()
				.filter_map(|id| Some(tg::Artifact::with_id(id.try_into().ok()?)))
				.chain(state.mounts.iter().filter_map(|mount| {
					if let tg::process::mount::Source::Artifact(artifact) = &mount.source {
						Some(artifact.clone())
					} else {
						None
					}
				}))
				.map(|artifact| async move {
					let arg = tg::artifact::checkout::Arg::default();
					artifact.check_out(&self.server, arg).await?;
					Ok::<_, tg::Error>(())
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await?;
		}

		// Create the instance.
		let instance = self
			.create_instance(process)
			.await
			.map_err(|source| tg::error!(!source, "failed to create runtime instance"))?;

		// Get the artifacts path.
		let artifacts_path = if instance.root_is_bind_mounted {
			self.server.artifacts_path()
		} else {
			instance.artifacts_path()
		};

		// Create the proxy for the chroot.
		let proxy = if !instance.root_is_bind_mounted {
			// Create the path map.
			let path_map = proxy::PathMap {
				output_host: instance.outdir(),
				output_guest: "/output".into(),
				root_host: instance.rootdir(),
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
			let socket = instance.rootdir().join(".tangram/socket");
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
		} else {
			None
		};

		// Render the args.
		let args = command.args(&self.server).await?;
		let args: Vec<String> = args
			.iter()
			.map(|value| async {
				let value = util::render(&self.server, value, &artifacts_path).await?;
				Ok::<_, tg::Error>(value)
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		// Get the working directory.
		let cwd = if let Some(cwd) = &state.cwd {
			cwd.clone()
		} else {
			"/".into()
		};

		// Get the command env.
		let command_env = command.env(&self.server).await?;

		// Get the process env.
		let process_env = state.env.as_ref();

		// Merge the environment.
		let mut env =
			super::util::merge_env(&self.server, &artifacts_path, process_env, &*command_env)
				.await?;

		// Render the executable.
		let Some(executable) = command.executable(&self.server).await?.as_ref().cloned() else {
			return Err(tg::error!("missing executable"));
		};
		let executable = match executable {
			tangram_client::command::Executable::Artifact(artifact) => {
				util::render(&self.server, &artifact.into(), &artifacts_path).await?
			},
			tangram_client::command::Executable::Module(_) => {
				return Err(tg::error!("invalid executable"));
			},
			tangram_client::command::Executable::Path(path_buf) => {
				path_buf.to_string_lossy().to_string()
			},
		};

		// Set `$OUTPUT`.
		if instance.root_is_bind_mounted {
			env.insert("OUTPUT".to_owned(), instance.outdir().display().to_string());
		} else {
			env.insert("OUTPUT".to_owned(), "/output/output".to_owned());
		}

		// Set `$TANGRAM_URL`.
		let url = proxy
			.as_ref()
			.map(|(_, url)| url.to_string())
			.unwrap_or_else(|| {
				let path = self.server.path.join("socket").display().to_string();
				let path = urlencoding::encode(&path);
				format!("http+unix://{path}")
			});
		env.insert("TANGRAM_URL".to_owned(), url.to_string());

		// Create the command
		let mut cmd_ = sandbox::Command::new(executable);
		cmd_.args(args)
			.chroot(instance.rootdir())
			.cwd(cwd)
			.envs(env)
			.mounts(instance.mounts.clone());

		// Setup stdio.
		cmd_.stdin(sandbox::Stdio::Piped);
		if let Some(tg::process::Io::Pty(pty)) = &state.stdin {
			let ws = self
				.server
				.get_pty_window_size(
					pty,
					tg::pty::get::Arg {
						remote: process.remote().cloned(),
						master: true,
					},
				)
				.await?
				.ok_or_else(|| tg::error!("failed to get pipe"))?;
			let tty = sandbox::Tty {
				rows: ws.rows,
				cols: ws.cols,
				x: ws.xpos,
				y: ws.ypos,
			};
			cmd_.stdin(sandbox::Stdio::Tty(tty));
		}

		cmd_.stdout(sandbox::Stdio::Piped);
		if let Some(tg::process::Io::Pty(pty)) = &state.stdout {
			let ws = self
				.server
				.get_pty_window_size(
					pty,
					tg::pty::get::Arg {
						remote: process.remote().cloned(),
						master: false,
					},
				)
				.await?
				.ok_or_else(|| tg::error!("failed to get pipe"))?;
			let tty = sandbox::Tty {
				rows: ws.rows,
				cols: ws.cols,
				x: ws.xpos,
				y: ws.ypos,
			};
			cmd_.stdout(sandbox::Stdio::Tty(tty));
		}

		cmd_.stderr(sandbox::Stdio::Piped);
		if let Some(tg::process::Io::Pty(pty)) = &state.stderr {
			let ws = self
				.server
				.get_pty_window_size(
					pty,
					tg::pty::get::Arg {
						remote: process.remote().cloned(),
						master: false,
					},
				)
				.await?
				.ok_or_else(|| tg::error!("failed to get pipe"))?;
			let tty = sandbox::Tty {
				rows: ws.rows,
				cols: ws.cols,
				x: ws.xpos,
				y: ws.ypos,
			};
			cmd_.stderr(sandbox::Stdio::Tty(tty));
		}

		// Spawn the child.
		let mut child = cmd_
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
		let stdin = child.stdin.take().unwrap();
		let stdout = child.stdout.take().unwrap();
		let stderr = child.stderr.take().unwrap();
		let stdio_task = Task::spawn(move |stop| {
			super::util::stdio_task(
				self.server.clone(),
				process.clone(),
				stop,
				stdin,
				stdout,
				stderr,
			)
		});

		// Spawn the signal task.
		let signal_task = tokio::spawn({
			let server = self.server.clone();
			let process = process.clone();
			let pid = child.pid();
			async move {
				util::signal_task(&server, pid, &process)
					.await
					.inspect_err(|source| tracing::error!(?source, "signal task failed"))
					.ok();
			}
		});

		// Wait for the child process to complete.
		let exit = child.wait().await;
		let exit =
			// Wrap the error and return.
			match exit.map_err(|source| tg::error!(!source, %process = process.id(), "failed to wait for the child process"))? {
				sandbox::ExitStatus::Code(code) => tg::process::Exit::Code { code },
				sandbox::ExitStatus::Signal(signal) => tg::process::Exit::Signal { signal },
			};

		// Stop the proxy task.
		if let Some(task) = proxy.map(|(proxy, _)| proxy) {
			task.stop();
			task.wait().await.unwrap();
		}

		// Stop the signal task.
		signal_task.abort();

		// stop the i/o task.
		stdio_task.stop();
		stdio_task.wait().await.unwrap().ok();

		// Create the output.
		let output = instance.outdir().join("output");
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

		// Drop the instance.
		drop(instance);

		Ok((Some(exit), output))
	}
}

impl Instance {
	pub fn rootdir(&self) -> PathBuf {
		self.temp.path().join("root")
	}

	pub fn outdir(&self) -> PathBuf {
		self.temp.path().join("output")
	}

	pub fn artifacts_path(&self) -> PathBuf {
		"/.tangram/artifacts".into()
	}
}

impl Runtime {
	async fn create_instance(&self, process: &tg::Process) -> tg::Result<Instance> {
		let state = process.load(&self.server).await?;
		let command_mounts = state.command.mounts(&self.server).await?;

		// Create the temp.
		let temp = Temp::new(&self.server);
		let mut instance = Instance {
			root_is_bind_mounted: false,
			temp,
			mounts: Vec::with_capacity(state.mounts.len() + command_mounts.len()),
		};

		// Create the proxy path.
		let path = instance.rootdir().join(".tangram");
		tokio::fs::create_dir_all(&path)
			.await
			.map_err(|source| {
				tg::error!(!source, %path = path.display(), "failed to create the tangram proxy directory")
			})?;

		// Create the output path.
		tokio::fs::create_dir_all(&instance.outdir())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the output directory"))?;

		// Create mounts.
		let mut overlays: Vec<sandbox::Overlay> = vec![];
		let command_mounts: Vec<tg::process::Mount> =
			command_mounts.iter().cloned().map(Into::into).collect();
		for mount in state.mounts.iter().chain(command_mounts.iter()) {
			match &mount.source {
				tg::process::mount::Source::Artifact(artifact) => {
					let overlay = 'a: {
						if let Some(overlay) = overlays
							.iter_mut()
							.find(|overlay| overlay.merged == mount.target)
						{
							break 'a overlay;
						};
						let upperdir = instance
							.temp
							.path()
							.join("upper")
							.join(overlays.len().to_string());
						let workdir = instance
							.temp
							.path()
							.join("work")
							.join(overlays.len().to_string());
						tokio::fs::create_dir_all(&upperdir).await.ok();
						tokio::fs::create_dir_all(&workdir).await.ok();
						overlays.push(sandbox::Overlay {
							lowerdirs: vec![],
							upperdir,
							workdir,
							merged: mount.target.clone(),
							readonly: false,
						});
						overlays.last_mut().unwrap()
					};
					overlay.lowerdirs.push(
						self.server
							.artifacts_path()
							.join(artifact.id(&self.server).await?.to_string()),
					);
				},
				tg::process::mount::Source::Path(path) => {
					instance.root_is_bind_mounted |= path == Path::new("/");
					instance.mounts.push(
						sandbox::BindMount {
							source: path.clone(),
							target: mount.target.clone(),
							readonly: mount.readonly,
						}
						.into(),
					);
				},
			}
		}
		instance
			.mounts
			.extend(overlays.into_iter().map(sandbox::Mount::from));

		if !instance.root_is_bind_mounted {
			// Add common mounts for /proc, /tmp, /dev, /output, /.tangram/artifacts
			instance.mounts.push(sandbox::Mount {
				source: "/proc".into(),
				target: "/proc".into(),
				fstype: Some("proc".into()),
				flags: 0,
				data: None,
				readonly: false,
			});
			instance.mounts.push(sandbox::Mount {
				source: "/tmp".into(),
				target: "/tmp".into(),
				fstype: Some("tmpfs".into()),
				flags: 0,
				data: None,
				readonly: false,
			});
			instance.mounts.push(
				sandbox::BindMount {
					source: "/dev".into(),
					target: "/dev".into(),
					readonly: false,
				}
				.into(),
			);
			instance.mounts.push(
				sandbox::Mount {
					source: self.server.artifacts_path(),
					target: "/.tangram/artifacts".into(),
					fstype: None,
					flags: libc::MS_BIND | libc::MS_REC,
					data: None,
					readonly: false,
				}
				.into(),
			);
			instance.mounts.push(
				sandbox::BindMount {
					source: instance.outdir(),
					target: "/output".into(),
					readonly: false,
				}
				.into(),
			);

			// Create /etc
			tokio::fs::create_dir_all(instance.rootdir().join("etc"))
				.await
				.ok();

			// Create nsswitch.conf.
			tokio::fs::write(
				instance.rootdir().join("etc/nsswitch.conf"),
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

			// Create /etc/passwd.
			tokio::fs::write(
				instance.rootdir().join("etc/passwd"),
				formatdoc!(
					r#"
						root:!:0:0:root:/nonexistent:/bin/false
						nobody:!:65534:65534:nobody:/nonexistent:/bin/false
					"#
				),
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to create /etc/passwd"))?;

			// Copy resolv.conf.
			if state.network {
				tokio::fs::copy(
					"/etc/resolv.conf",
					instance.rootdir().join("etc/resolv.conf"),
				)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to copy /etc/resolv.conf to sandbox")
				})?;
			}
		}

		instance
			.mounts
			.sort_unstable_by_key(|m| m.target.components().count());

		// Return the instance.
		Ok(instance)
	}
}
