use {
	super::util::{
		cache_children, render_args_dash_a, render_args_string, render_env, which, whoami,
	},
	crate::{Context, Server, temp::Temp},
	indoc::formatdoc,
	std::{
		collections::{BTreeMap, HashMap},
		os::unix::ffi::OsStrExt as _,
		path::{Path, PathBuf},
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_either::Either,
	tangram_futures::task::Task,
	tangram_uri::Uri,
};

struct SandboxArg<'a> {
	args: &'a [String],
	command: &'a tg::command::Data,
	cwd: &'a Path,
	env: &'a BTreeMap<String, String>,
	executable: &'a Path,
	id: &'a tg::process::Id,
	mounts: &'a [Either<&'a tg::process::Mount, &'a tg::command::data::Mount>],
	root_mounted: bool,
	server: &'a Server,
	state: &'a tg::process::State,
	temp: &'a Temp,
}

struct SandboxOutput {
	args: Vec<String>,
	cwd: PathBuf,
	env: BTreeMap<String, String>,
	executable: PathBuf,
	root: PathBuf,
}

impl Server {
	pub(crate) async fn run_linux(&self, process: &tg::Process) -> tg::Result<super::Output> {
		let id = process.id();
		let state = &process.load(self).await?;
		let remote = process.remote();

		let command = process.command(self).await?;
		let command = &command.data(self).await?;

		// Cache the process's chidlren.
		cache_children(self, process).await?;

		// Create the temp.
		let temp = Temp::new(self);
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the temp directory"))?;

		// Create the output directory.
		tokio::fs::create_dir_all(temp.path().join("output"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the output directory"))?;

		// Determine if the root is mounted.
		let root_mounted = state
			.mounts
			.iter()
			.any(|mount| mount.source == mount.target && mount.target == Path::new("/"));

		// Get the artifacts path.
		let artifacts_path = if root_mounted {
			self.artifacts_path()
		} else {
			"/.tangram/artifacts".into()
		};

		// Render the args.
		let mut args = match command.host.as_str() {
			"builtin" | "js" => render_args_dash_a(&command.args),
			_ => render_args_string(&command.args, &artifacts_path),
		};

		// Get the working directory.
		let cwd = if let Some(cwd) = &command.cwd {
			cwd.clone()
		} else {
			"/".into()
		};

		// Render the env.
		let mut env = render_env(&command.env, &artifacts_path)?;

		// Render the executable.
		let executable = match command.host.as_str() {
			"builtin" => {
				let tg = tangram_util::env::current_exe().map_err(|source| {
					tg::error!(!source, "failed to get the current executable")
				})?;
				args.insert(0, "builtin".to_owned());
				args.insert(1, command.executable.to_string());
				tg
			},

			"js" => {
				let tg = tangram_util::env::current_exe().map_err(|source| {
					tg::error!(!source, "failed to get the current executable")
				})?;
				args.insert(0, "js".to_owned());
				args.insert(1, command.executable.to_string());
				tg
			},

			_ => match &command.executable {
				tg::command::data::Executable::Artifact(executable) => {
					let mut path = artifacts_path.join(executable.artifact.to_string());
					if let Some(executable_path) = &executable.path {
						path.push(executable_path);
					}
					path
				},
				tg::command::data::Executable::Module(_) => {
					return Err(tg::error!("invalid executable"));
				},
				tg::command::data::Executable::Path(executable) => {
					which(&executable.path, &env).await?
				},
			},
		};

		// Create mounts.
		let mounts = std::iter::empty()
			.chain(state.mounts.iter().map(Either::Left))
			.chain(command.mounts.iter().map(Either::Right))
			.collect::<Vec<_>>();

		// Set `$OUTPUT`.
		let path = if root_mounted {
			temp.path().join("output/output")
		} else {
			Path::new("/output/output").to_owned()
		};
		env.insert("OUTPUT".to_owned(), path.to_str().unwrap().to_owned());

		// Set `$TANGRAM_PROCESS`.
		env.insert("TANGRAM_PROCESS".to_owned(), id.to_string());

		// Set `$TANGRAM_URL`.
		let path = if root_mounted {
			self.path.join("socket")
		} else {
			Path::new("/.tangram/socket").to_owned()
		};
		let path = path.to_str().unwrap();
		let path = urlencoding::encode(path);
		let url = format!("http+unix://{path}");
		env.insert("TANGRAM_URL".to_owned(), url);

		// Get the server's user.
		let whoami = whoami().map_err(|error| tg::error!(!error, "failed to get username"))?;

		// Determine if the process is sandboxed.
		let unsandboxed = root_mounted
			&& (command.mounts.is_empty() && state.mounts.len() == 1)
			&& state.network
			&& command.user.as_ref().is_none_or(|user| user == &whoami);

		// Sandbox if necessary.
		let (args, cwd, env, executable, root) = if unsandboxed {
			let root = temp.path().join("root");
			(args, cwd, env, executable, root)
		} else {
			let arg = SandboxArg {
				args: &args,
				command,
				cwd: &cwd,
				env: &env,
				executable: &executable,
				id,
				mounts: &mounts,
				root_mounted,
				server: self,
				state,
				temp: &temp,
			};
			let output = sandbox(arg).await?;
			let SandboxOutput {
				args,
				cwd,
				env,
				executable,
				root,
			} = output;
			(args, cwd, env, executable, root)
		};

		// Create the serve task.
		let serve_task = if root_mounted {
			None
		} else {
			// Create the paths.
			let paths = crate::context::Paths {
				server_host: self.path.clone(),
				output_host: temp.path().join("output"),
				output_guest: "/output".into(),
				root_host: root,
			};

			// Create the guest uri.
			let socket = Path::new("/.tangram/socket");
			let socket = socket.to_str().unwrap();
			let socket = urlencoding::encode(socket);
			let guest_uri = format!("http+unix://{socket}").parse::<Uri>().unwrap();

			// Create the host uri.
			let socket = temp.path().join(".tangram/socket");
			tokio::fs::create_dir_all(socket.parent().unwrap())
				.await
				.map_err(|source| tg::error!(!source, "failed to create the host path"))?;
			let socket = urlencoding::encode(
				socket
					.to_str()
					.ok_or_else(|| tg::error!(path = %socket.display(), "invalid path"))?,
			);
			let host_uri = format!("http+unix://{socket}").parse::<Uri>().unwrap();

			// Listen.
			let listener = Server::listen(&host_uri).await?;

			// Serve.
			let server = self.clone();
			let context = Context {
				process: Some(Arc::new(crate::context::Process {
					id: process.id().clone(),
					paths: Some(paths),
					remote: remote.cloned(),
					retry: *process.retry(self).await?,
				})),
				..Default::default()
			};
			let task =
				Task::spawn(|stop| async move { server.serve(listener, context, stop).await });

			Some((task, guest_uri))
		};

		// Run the process.
		let arg = crate::run::common::Arg {
			args,
			command,
			cwd,
			env,
			executable,
			id,
			remote,
			serve_task,
			server: self,
			state,
			temp: &temp,
		};
		let output = crate::run::common::run(arg).await?;

		Ok(output)
	}
}

async fn sandbox(arg: SandboxArg<'_>) -> tg::Result<SandboxOutput> {
	let SandboxArg {
		args,
		cwd,
		env,
		executable,
		id,
		mounts,
		root_mounted,
		server,
		temp,
		state,
		command,
	} = arg;

	// Create the output/root paths.
	let output_path = temp.path().join("output");

	// Initialize the output with all fields.
	let mut output = SandboxOutput {
		root: temp.path().join("root"),
		args: vec!["sandbox".to_owned()],
		cwd: PathBuf::from("/"),
		env: BTreeMap::new(),
		executable: tangram_util::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the current executable"))?,
	};

	let mut overlays = HashMap::new();
	for mount in mounts {
		match mount {
			Either::Left(mount) => {
				output.args.push("--mount".to_owned());
				output
					.args
					.push(bind(&mount.source, &mount.target, mount.readonly));
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
						output.root = upperdir.clone();
					}
					overlays.insert(mount.target.clone(), (lowerdirs, upperdir, workdir));
				}

				// Compute the path.
				let path = server.artifacts_path().join(mount.source.to_string());

				// Get the lower dirs.
				let (lowerdirs, _, _) = overlays.get_mut(&mount.target).unwrap();

				// Add this path to the lowerdirs.
				lowerdirs.push(path);
			},
		}
	}

	// Add additional mounts.
	if !root_mounted {
		let path = temp.path().join(".tangram");
		tokio::fs::create_dir_all(&path).await.map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to create the data directory"),
		)?;

		// Create /etc.
		tokio::fs::create_dir_all(temp.path().join("lower/etc"))
			.await
			.ok();

		// Create /tmp.
		tokio::fs::create_dir_all(temp.path().join("lower/tmp"))
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
				tg::error!(!source, "failed to copy /etc/resolv.conf to the sandbox")
			})?;
			output.args.push("--network".to_owned());
		}

		// Get or create the root overlay.
		if !overlays.contains_key(Path::new("/")) {
			let lowerdirs = Vec::new();
			let upperdir = temp.path().join("upper").join(overlays.len().to_string());
			let workdir = temp.path().join("work").join(overlays.len().to_string());
			tokio::fs::create_dir_all(&upperdir).await.ok();
			tokio::fs::create_dir_all(&workdir).await.ok();
			output.root = upperdir.clone();
			overlays.insert("/".into(), (lowerdirs, upperdir, workdir));
		}
		let (lowerdirs, _, _) = overlays.get_mut(Path::new("/")).unwrap();
		lowerdirs.push(temp.path().join("lower"));

		// Add mounts for /dev, /proc, /tmp, /.tangram, /.tangram/artifacts, and /output.
		output.args.push("--mount".to_owned());
		output.args.push(bind("/dev", "/dev", false));
		output.args.push("--mount".to_owned());
		output.args.push(bind("/proc", "/proc", false));
		output.args.push("--mount".to_owned());
		output
			.args
			.push(bind(temp.path().join(".tangram"), "/.tangram", false));
		output.args.push("--mount".to_owned());
		output
			.args
			.push(bind(server.artifacts_path(), "/.tangram/artifacts", true));
		output.args.push("--mount".to_owned());
		output.args.push(bind(&output_path, "/output", false));
	}

	// Add the overlay mounts.
	for (merged, (lowerdirs, upperdir, workdir)) in &overlays {
		output.args.push("--mount".to_owned());
		output
			.args
			.push(overlay(lowerdirs, upperdir, workdir, merged));
	}

	output.args.push("-C".to_owned());
	output.args.push(cwd.display().to_string());

	// Add env vars to the command.
	for (name, value) in env {
		output.args.push("-e".to_owned());
		output.args.push(format!("{name}={value}"));
	}

	// Set the user.
	let user = command.user.as_deref().unwrap_or("root");
	output.args.push("--user".to_owned());
	output.args.push(user.to_owned());

	// Set the hostname
	output.args.push("--hostname".to_owned());
	output.args.push(id.to_string());

	// Set the chroot.
	output.args.push("--chroot".to_owned());
	output.args.push(output.root.display().to_string());

	// Add the executable and original args.
	output.args.push(executable.display().to_string());
	output.args.push("--".to_owned());
	output.args.extend(args.iter().cloned());

	Ok(output)
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
