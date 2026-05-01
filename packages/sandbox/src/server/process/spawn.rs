use {
	crate::{
		Command, Stdio, abort_errno,
		pty::Pty,
		server::{Process, Server},
	},
	std::{
		os::{
			fd::{AsRawFd as _, OwnedFd},
			unix::process::ExitStatusExt as _,
		},
		path::Path,
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext, builder::Ext as _},
	},
	tokio::sync::Mutex,
};

#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "linux")]
mod linux;

impl Server {
	pub async fn spawn(
		&self,
		mut arg: crate::client::spawn::Arg,
	) -> tg::Result<crate::client::spawn::Output> {
		for key in [
			"TANGRAM_CONFIG",
			"TANGRAM_DIRECTORY",
			"TANGRAM_MODE",
			"TANGRAM_OUTPUT",
			"TANGRAM_PROCESS",
			"TANGRAM_TOKEN",
			"TANGRAM_TRACING",
			"TANGRAM_URL",
		] {
			arg.command.env.remove(key);
		}
		arg.command.env.insert(
			"TANGRAM_OUTPUT".to_owned(),
			self.output_path
				.join(arg.id.to_string())
				.to_str()
				.unwrap()
				.to_owned(),
		);
		arg.command
			.env
			.insert("TANGRAM_PROCESS".to_owned(), arg.id.to_string());
		arg.command
			.env
			.insert("TANGRAM_URL".to_owned(), arg.url.to_string());

		// Apply host specific modifications to the command.
		#[cfg(target_os = "macos")]
		{
			self::darwin::prepare_command_for_spawn(
				&mut arg.command,
				&self.tangram_path,
				&self.library_paths,
			)?;
		}
		#[cfg(target_os = "linux")]
		{
			self::linux::prepare_command_for_spawn(
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
			.kill_on_drop(true)
			.current_dir(&arg.command.cwd)
			.env_clear()
			.envs(&arg.command.env)
			.args(&arg.command.args)
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
					start_session(&tty, stdin_is_tty, stdout_is_tty, stderr_is_tty);
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

		// Spawn the task.
		let task = tangram_futures::task::Shared::spawn(move |_| async move {
			child
				.wait()
				.await
				.map_err(|source| tg::error!(!source, "failed to wait for the process"))
				.map(|status| {
					status
						.code()
						.or(status.signal().map(|signal| 128 + signal))
						.unwrap_or(1)
						.try_into()
						.unwrap_or(u8::MAX)
				})
		});

		// Insert the process.
		let process = Process {
			command: arg.command,
			debug: arg.debug,
			location: arg.location,
			pid,
			retry: arg.retry,
			stdin,
			stdout,
			stderr,
			pty: pty.map(Arc::new),
			task,
		};
		self.processes.insert(arg.id.clone(), process);

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

fn append_directories_to_path(command: &mut Command, directories: &[&Path]) -> tg::Result<()> {
	let mut paths = command
		.env
		.get("PATH")
		.map(|path| std::env::split_paths(path).collect::<Vec<_>>())
		.unwrap_or_default();
	paths.extend(directories.iter().map(|path| path.to_path_buf()));
	let path = std::env::join_paths(paths)
		.map_err(|source| tg::error!(!source, "failed to build `PATH`"))?;
	let path = path
		.to_str()
		.ok_or_else(|| tg::error!("failed to encode `PATH` as valid UTF-8"))?;
	command.env.insert("PATH".to_owned(), path.to_owned());
	Ok(())
}

fn child_stdio(stdio: Stdio, pty: Option<&Pty>) -> tg::Result<std::process::Stdio> {
	match stdio {
		Stdio::Null => {
			let stdio = std::process::Stdio::null();
			Ok(stdio)
		},
		Stdio::Pipe => {
			let stdio = std::process::Stdio::piped();
			Ok(stdio)
		},
		Stdio::Tty => {
			let fd = pty
				.and_then(Pty::slave)
				.ok_or_else(|| tg::error!("expected a tty arg"))?
				.try_clone()
				.map_err(|source| tg::error!(!source, "failed to clone the tty fd"))?;
			let stdio = std::process::Stdio::from(fd);
			Ok(stdio)
		},
	}
}

fn start_session(tty: &OwnedFd, stdin: bool, stdout: bool, stderr: bool) {
	unsafe {
		let current_tty = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
		if current_tty >= 0 {
			#[cfg_attr(target_os = "linux", expect(clippy::useless_conversion))]
			libc::ioctl(
				current_tty,
				libc::TIOCNOTTY.into(),
				std::ptr::null_mut::<()>(),
			);
			libc::close(current_tty);
		}

		let ret = libc::setsid();
		if ret < 0 {
			abort_errno!("setsid() failed");
		}

		let fd = tty.as_raw_fd();
		#[cfg_attr(target_os = "linux", expect(clippy::useless_conversion))]
		let ret = libc::ioctl(fd, libc::TIOCSCTTY.into(), 0);
		if ret < 0 {
			abort_errno!("failed to set the controlling terminal");
		}

		if stdin {
			libc::dup2(fd, libc::STDIN_FILENO);
		}
		if stdout {
			libc::dup2(fd, libc::STDOUT_FILENO);
		}
		if stderr {
			libc::dup2(fd, libc::STDERR_FILENO);
		}

		if fd > libc::STDERR_FILENO {
			let flags = libc::fcntl(fd, libc::F_GETFD);
			if flags >= 0 {
				libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
			}
		}
	}
}
