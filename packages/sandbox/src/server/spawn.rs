use {
	crate::{
		Stdio,
		pty::Pty,
		server::{Process, Server},
	},
	std::{path::Path, sync::Arc},
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
		arg: crate::client::spawn::Arg,
	) -> tg::Result<crate::client::spawn::Output> {
		let mut arg = arg;
		crate::prepare_command_for_spawn(
			&mut arg.command,
			&self.tangram_path,
			&self.library_paths,
		)?;
		if !arg.command.cwd.is_absolute() {
			return Err(tg::error!(
				cwd = %arg.command.cwd.display(),
				"the working directory must be an absolute path"
			));
		}

		let stdin_is_tty = matches!(arg.command.stdin, Stdio::Tty);
		let stdout_is_tty = matches!(arg.command.stdout, Stdio::Tty);
		let stderr_is_tty = matches!(arg.command.stderr, Stdio::Tty);
		let uses_tty = stdin_is_tty || stdout_is_tty || stderr_is_tty;
		let mut pty = arg.tty.map(Pty::new).transpose()?;
		if uses_tty && pty.is_none() {
			return Err(tg::error!("expected a tty arg"));
		}

		let executable = resolve_executable(&arg.command);
		let mut command = tokio::process::Command::new(&executable);
		command
			.env_clear()
			.args(&arg.command.args)
			.envs(&arg.command.env)
			.current_dir(&arg.command.cwd)
			.stdin(child_stdio(arg.command.stdin, pty.as_ref())?)
			.stdout(child_stdio(arg.command.stdout, pty.as_ref())?)
			.stderr(child_stdio(arg.command.stderr, pty.as_ref())?);

		if uses_tty {
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

		let mut child = command.spawn().map_err(|source| {
			tg::error!(
				!source,
				executable = %executable.display(),
				"failed to spawn the child process"
			)
		})?;
		let pid = child
			.id()
			.and_then(|pid| i32::try_from(pid).ok())
			.ok_or_else(|| tg::error!("failed to get the process id"))?;

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

		if let Some(pty) = &mut pty {
			pty.take_slave();
		}
		let id = arg.id.clone();
		self.processes.insert(
			id.clone(),
			Process {
				command: arg.command,
				notify: Arc::new(tokio::sync::Notify::new()),
				pid,
				stdin,
				stdout,
				stderr,
				pty: pty.map(Arc::new),
				status: None,
			},
		);
		let server = self.clone();
		let process_id = id.clone();
		tokio::spawn(async move {
			let status = child
				.wait()
				.await
				.map_err(|source| tg::error!(!source, "failed to wait for the process"))
				.map(exit_status_to_code);
			if let Some(mut process) = server.processes.get_mut(&process_id) {
				process.status.replace(status);
				process.notify.notify_waiters();
			}
		});

		Ok(crate::client::spawn::Output { id })
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

fn resolve_executable(command: &crate::Command) -> std::path::PathBuf {
	command
		.env
		.get("PATH")
		.and_then(|path| crate::util::which(Path::new(path), &command.executable))
		.unwrap_or_else(|| command.executable.clone())
}

fn exit_status_to_code(status: std::process::ExitStatus) -> u8 {
	use std::os::unix::process::ExitStatusExt as _;

	status
		.code()
		.or(status.signal().map(|signal| 128 + signal))
		.unwrap_or(1)
		.try_into()
		.unwrap_or(u8::MAX)
}
