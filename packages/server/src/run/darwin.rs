use {
	super::{
		proxy::Proxy,
		util::{cache_children, render_env, render_value, which},
	},
	crate::{Server, run::util::whoami, temp::Temp},
	std::{
		collections::BTreeMap,
		path::{Path, PathBuf},
	},
	tangram_client as tg,
	tangram_futures::task::Task,
	tangram_uri::Uri,
};

const MAX_URL_LEN: usize = 100;

impl Server {
	pub(crate) async fn run_darwin(&self, process: &tg::Process) -> tg::Result<super::Output> {
		let id = process.id();
		let state = &process.load(self).await?;
		let remote = process.remote();

		let command = process.command(self).await?;
		let command = &command.data(self).await?;

		// Cache the process's children.
		cache_children(self, process).await?;

		// Determine if the root is mounted.
		let root_mounted = state
			.mounts
			.iter()
			.any(|mount| mount.source == mount.target && mount.target == Path::new("/"));

		// Get the artifacts path.
		let artifacts_path = self.artifacts_path();

		// Create the temp.
		let temp = Temp::new(self);
		tokio::fs::create_dir_all(&temp)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the temp directory"))?;

		// Create the output directory.
		tokio::fs::create_dir_all(temp.path().join("output"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the output directory"))?;

		// Render the args.
		let args: Vec<String> = command
			.args
			.iter()
			.map(|value| render_value(&artifacts_path, value))
			.collect();

		// Create the working directory.
		let cwd = command
			.cwd
			.clone()
			.unwrap_or_else(|| temp.path().join("work"));
		tokio::fs::create_dir_all(&cwd)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the working directory"))?;

		// Render the env.
		let mut env = render_env(&artifacts_path, &command.env)?;

		// Render the executable.
		let executable = match &command.executable {
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
		};

		// Create the proxy.
		let proxy = if root_mounted {
			None
		} else {
			let path = temp.path().join(".tangram");
			tokio::fs::create_dir_all(&path)
				.await
				.map_err(|source| tg::error!(!source, %path = path.display(), "failed to create the proxy server directory"))?;
			let proxy = Proxy::new(self.clone(), process, remote.cloned(), None);
			let socket_path = path.join("socket").display().to_string();
			let mut url = if socket_path.len() >= MAX_URL_LEN {
				let path = urlencoding::encode(&socket_path);
				format!("http+unix://{path}").parse::<Uri>().unwrap()
			} else {
				"http://localhost:0".to_string().parse::<Uri>().unwrap()
			};
			let listener = Server::listen(&url).await?;
			let listener_addr = listener
				.local_addr()
				.map_err(|source| tg::error!(!source, "failed to get listener address"))?;
			if let tokio_util::either::Either::Right(listener) = listener_addr {
				let port = listener.port();
				url = format!("http://localhost:{port}").parse::<Uri>().unwrap();
			}
			let task = Task::spawn(|stop| Server::serve(proxy, listener, stop));
			Some((task, url))
		};

		// Set `$OUTPUT`.
		let path = temp.path().join("output/output");
		env.insert("OUTPUT".to_owned(), path.to_str().unwrap().to_owned());

		// Set `$TANGRAM_PROCESS`.
		env.insert("TANGRAM_PROCESS".to_owned(), id.to_string());

		// Set `$TANGRAM_URL`.
		let url = proxy.as_ref().map_or_else(
			|| {
				let path = self.path.join("socket");
				let path = path.to_str().unwrap();
				let path = urlencoding::encode(path);
				format!("http+unix://{path}")
			},
			|(_, url)| url.to_string(),
		);
		env.insert("TANGRAM_URL".to_owned(), url);

		// Get the server's user.
		let whoami = whoami().map_err(|error| tg::error!(!error, "failed to get username"))?;

		// Determine if the process is sandboxed.
		let unsandboxed = root_mounted
			&& (command.mounts.is_empty() && state.mounts.len() == 1)
			&& state.network
			&& command.user.as_ref().is_none_or(|user| user == &whoami);

		// Sandbox if necessary.
		let (args, cwd, env, executable) = if unsandboxed {
			(args, cwd, env, executable)
		} else {
			let args = {
				let mut args_ = vec!["sandbox".to_owned()];
				args_.push("-C".to_owned());
				args_.push(cwd.display().to_string());
				for (name, value) in &env {
					args_.push("-e".to_owned());
					args_.push(format!("{name}={value}"));
				}
				if !root_mounted {
					args_.push("--mount".to_owned());
					args_.push(format!("source={}", temp.path().display()));
					args_.push("--mount".to_owned());
					args_.push(format!("source={},ro", artifacts_path.display()));
					args_.push("--mount".to_owned());
					args_.push(format!("source={}", cwd.display()));
				}
				for mount in &state.mounts {
					let mount = if mount.readonly {
						format!("source={},ro", mount.source.display())
					} else {
						format!("source={}", mount.source.display())
					};
					args_.push("--mount".to_owned());
					args_.push(mount);
				}
				if state.network {
					args_.push("--network".to_owned());
				}
				args_.push(executable.display().to_string());
				args_.push("--".to_owned());
				args_.extend(args);
				args_
			};
			let cwd = PathBuf::from("/");
			let env = BTreeMap::new();
			let executable = std::env::current_exe()
				.map_err(|source| tg::error!(!source, "failed to get the current executable"))?;
			(args, cwd, env, executable)
		};

		// Run the process.
		let arg = crate::run::common::Arg {
			args,
			command,
			cwd,
			env,
			executable,
			id,
			proxy,
			remote,
			server: self,
			state,
			temp: &temp,
		};
		let output = crate::run::common::run(arg).await?;

		Ok(output)
	}
}
