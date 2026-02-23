use {
	super::util::{cache_children, render_args_dash_a, render_args_string, render_env},
	crate::{Context, Server, temp::Temp},
	std::{path::Path, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

impl Server {
	pub(crate) async fn run_linux(&self, process: &tg::Process) -> tg::Result<super::Output> {
		let id = process.id();
		let state = &process
			.load(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to load the process"))?;
		let remote = process.remote();

		let command = process
			.command(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the command"))?;
		let command = &command
			.data(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the command data"))?;

		// Cache the process's chidlren.
		cache_children(self, process)
			.await
			.map_err(|source| tg::error!(!source, "failed to cache the children"))?;

		// Get the artifacts path.
		let artifacts_path = if state.sandbox.is_none() {
			self.artifacts_path()
		} else {
			"/.tangram/artifacts".into()
		};

		// Get the output path.
		let temp = Temp::new(self);
		let output_path = if state.sandbox.is_none() {
			tokio::fs::create_dir_all(temp.path())
				.await
				.map_err(|source| tg::error!(!source, "failed to create output directory"))?;
			temp.path().join("output")
		} else {
			Path::new("/output/output").to_owned()
		};

		// Render the args.
		let mut args = match command.host.as_str() {
			"builtin" | "js" => render_args_dash_a(&command.args),
			_ => render_args_string(&command.args, &artifacts_path, &output_path)?,
		};

		// Get the working directory.
		let cwd = if let Some(cwd) = &command.cwd {
			cwd.clone()
		} else {
			"/".into()
		};

		// Render the env.
		let mut env = render_env(&command.env, &artifacts_path, &output_path)?;

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
				tg::command::data::Executable::Path(executable) => executable.path.clone(),
			},
		};

		// Set `$TANGRAM_OUTPUT`.
		env.insert(
			"TANGRAM_OUTPUT".to_owned(),
			output_path.to_str().unwrap().to_owned(),
		);

		// Set `$TANGRAM_PROCESS`.
		env.insert("TANGRAM_PROCESS".to_owned(), id.to_string());

		// Create the guest uri.
		let guest_socket = if state.sandbox.is_none() {
			self.path.join("socket")
		} else {
			Path::new("/.tangram/socket").to_owned()
		};
		let guest_socket = guest_socket.to_str().unwrap();
		let guest_uri = tangram_uri::Uri::builder()
			.scheme("http+unix")
			.authority(guest_socket)
			.path("")
			.build()
			.unwrap();

		// Set `$TANGRAM_URL`.
		env.insert("TANGRAM_URL".to_owned(), guest_uri.to_string());

		// Create the paths for sandboxed processes.
		let paths = None;

		// Create the host uri.
		let host_socket = temp.path().join(".tangram/socket");
		tokio::fs::create_dir_all(host_socket.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the host path"))?;
		let host_socket = host_socket
			.to_str()
			.ok_or_else(|| tg::error!(path = %host_socket.display(), "invalid path"))?;
		let host_uri = tangram_uri::Uri::builder()
			.scheme("http+unix")
			.authority(host_socket)
			.path("")
			.build()
			.unwrap();

		// Listen.
		let listener = Server::listen(&host_uri)
			.await
			.map_err(|source| tg::error!(!source, "failed to listen"))?;

		// Serve.
		let server = self.clone();
		let context = Context {
			process: Some(Arc::new(crate::context::Process {
				id: process.id().clone(),
				paths,
				remote: remote.cloned(),
				retry: *process
					.retry(self)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the process retry"))?,
				sandbox: state.sandbox.clone(),
			})),
			..Default::default()
		};
		let task = Task::spawn({
			let context = context.clone();
			|stop| async move {
				server.serve(listener, context, stop).await;
			}
		});

		let serve_task = Some((task, guest_uri));

		// Run the process.
		let arg = crate::run::common::Arg {
			args,
			command,
			context: &context,
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
		let output = crate::run::common::run(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to run the process"))?;

		Ok(output)
	}
}
