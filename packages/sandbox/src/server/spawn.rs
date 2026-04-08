use {
	crate::{
		Stdio,
		pty::Pty,
		server::{Process, Server},
	},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext, builder::Ext as _},
	},
	tokio::sync::Mutex,
};

impl Server {
	pub async fn spawn(
		&self,
		mut arg: crate::client::spawn::Arg,
	) -> tg::Result<crate::client::spawn::Output> {
		// Apply host specific modifications to the command.
		#[cfg(target_os = "macos")]
		{
			crate::darwin::prepare_command_for_spawn(
				&mut arg.command,
				&self.tangram_path,
				&self.library_paths,
			)?;
		}
		#[cfg(target_os = "linux")]
		{
			crate::linux::prepare_command_for_spawn(
				&mut arg.command,
				&self.tangram_path,
				&self.library_paths,
			)?;
		}

		// Validate the cwd.
		if !arg.command.cwd.is_absolute() {
			return Err(tg::error!(
				cwd = %arg.command.cwd.display(),
				"the working directory must be an absolute path"
			));
		}

		// Create the pty if necessary.
		let stdin_is_tty = matches!(arg.command.stdin, Stdio::Tty);
		let stdout_is_tty = matches!(arg.command.stdout, Stdio::Tty);
		let stderr_is_tty = matches!(arg.command.stderr, Stdio::Tty);
		let tty = stdin_is_tty || stdout_is_tty || stderr_is_tty;
		let mut pty = arg.tty.map(Pty::new).transpose()?;
		if tty && pty.is_none() {
			return Err(tg::error!("expected a tty arg"));
		}

		// Create the command.
		let executable = arg
			.command
			.env
			.get("PATH")
			.and_then(|path| {
				crate::util::which(std::path::Path::new(path), &arg.command.executable)
			})
			.unwrap_or_else(|| arg.command.executable.clone());
		let mut command = tokio::process::Command::new(&executable);
		command
			.env_clear()
			.args(&arg.command.args)
			.envs(&arg.command.env)
			.current_dir(&arg.command.cwd)
			.stdin(child_stdio(arg.command.stdin, pty.as_ref())?)
			.stdout(child_stdio(arg.command.stdout, pty.as_ref())?)
			.stderr(child_stdio(arg.command.stderr, pty.as_ref())?);

		// Set the pre_exec to start the session if there is a tty.
		if tty {
			let tty = pty
				.as_ref()
				.and_then(Pty::slave)
				.ok_or_else(|| tg::error!("expected a tty arg"))?
				.try_clone()
				.map_err(|source| tg::error!(!source, "failed to clone the tty fd"))?;
			unsafe {
				command.pre_exec(move || {
					crate::util::start_session(&tty, stdin_is_tty, stdout_is_tty, stderr_is_tty);
					Ok(())
				});
			}
		}

		// Spawn.
		let mut child = command.spawn().map_err(|source| {
			tg::error!(
				!source,
				executable = %executable.display(),
				"failed to spawn the child process"
			)
		})?;

		// Get the pid.
		let pid = child
			.id()
			.and_then(|pid| i32::try_from(pid).ok())
			.ok_or_else(|| tg::error!("failed to get the process id"))?;

		// Get stdio.
		let stdin = match arg.command.stdin {
			Stdio::Null | Stdio::Tty => None,
			Stdio::Pipe => {
				let stdin = child
					.stdin
					.take()
					.ok_or_else(|| tg::error!("failed to capture stdin"))?;
				Some(Arc::new(Mutex::new(stdin)))
			},
		};
		let stdout = match arg.command.stdout {
			Stdio::Null | Stdio::Tty => None,
			Stdio::Pipe => {
				let stdout = child
					.stdout
					.take()
					.ok_or_else(|| tg::error!("failed to capture stdout"))?;
				Some(Arc::new(Mutex::new(stdout)))
			},
		};
		let stderr = match arg.command.stderr {
			Stdio::Null | Stdio::Tty => None,
			Stdio::Pipe => {
				let stderr = child
					.stderr
					.take()
					.ok_or_else(|| tg::error!("failed to capture stderr"))?;
				Some(Arc::new(Mutex::new(stderr)))
			},
		};

		// Get the pty.
		if let Some(pty) = &mut pty {
			pty.take_slave();
		}

		let task = tangram_futures::task::Shared::spawn(move |_| async move {
			child
				.wait()
				.await
				.map_err(|source| tg::error!(!source, "failed to wait for the process"))
				.map(|status| {
					use std::os::unix::process::ExitStatusExt as _;

					status
						.code()
						.or(status.signal().map(|signal| 128 + signal))
						.unwrap_or(1)
						.try_into()
						.unwrap_or(u8::MAX)
				})
		});

		self.processes.insert(
			arg.id.clone(),
			Process {
				command: arg.command,
				pid,
				remote: arg.remote,
				retry: arg.retry,
				stdin,
				stdout,
				stderr,
				pty: pty.map(Arc::new),
				task,
			},
		);

		Ok(crate::client::spawn::Output {})
	}

	pub(crate) async fn handle_spawn_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;
		let output = self
			.spawn(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to spawn"))?;
		let response = http::Response::builder()
			.json(output)
			.unwrap()
			.unwrap()
			.boxed_body();
		Ok(response)
	}
}

fn child_stdio(stdio: Stdio, pty: Option<&Pty>) -> tg::Result<std::process::Stdio> {
	match stdio {
		Stdio::Null => Ok(std::process::Stdio::null()),
		Stdio::Pipe => Ok(std::process::Stdio::piped()),
		Stdio::Tty => {
			let fd = pty
				.and_then(Pty::slave)
				.ok_or_else(|| tg::error!("expected a tty arg"))?
				.try_clone()
				.map_err(|source| tg::error!(!source, "failed to clone the tty fd"))?;
			Ok(std::process::Stdio::from(fd))
		},
	}
}
