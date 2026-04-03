use {
	crate::{
		Stdio,
		common::{AsyncPtyFd, InputStream, OutputStream, Pty, SpawnContext},
		server::{Process, Server},
	},
	std::{fs::File, os::fd::OwnedFd, sync::Arc},
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

		// Create a PTY if a tty was requested.
		let mut pty = arg.tty.map(Pty::new).transpose()?;
		let (host_stdin, guest_stdin): (InputStream, Option<OwnedFd>) = match arg.command.stdin {
			Stdio::Null => {
				let fd = File::options()
					.read(true)
					.open("/dev/null")
					.map_err(|source| tg::error!(!source, "failed to open /dev/null"))?
					.into();
				(InputStream::Null, Some(fd))
			},
			Stdio::Pipe => {
				let (reader, writer) = std::io::pipe()
					.map_err(|source| tg::error!(!source, "failed to open a pipe"))?;
				let writer = tokio::net::unix::pipe::Sender::from_owned_fd(writer.into())
					.map_err(|source| tg::error!(!source, "failed to convert the pipe fd"))?;
				(InputStream::Pipe(writer), Some(reader.into()))
			},
			Stdio::Tty => {
				let pty = pty
					.as_ref()
					.ok_or_else(|| tg::error!("expected a tty arg"))?;
				let master = pty
					.master
					.try_clone()
					.map_err(|source| tg::error!(!source, "failed to clone the master fd"))?;
				let master = AsyncPtyFd::new(master)
					.map_err(|source| tg::error!(!source, "failed to create the async pty fd"))?;
				(InputStream::Pty(master), None)
			},
		};

		let (host_stdout, guest_stdout): (OutputStream, Option<OwnedFd>) = match arg.command.stdout
		{
			Stdio::Null => {
				let fd = File::options()
					.write(true)
					.open("/dev/null")
					.map_err(|source| tg::error!(!source, "failed to open /dev/null"))?
					.into();
				(OutputStream::Null, Some(fd))
			},
			Stdio::Pipe => {
				let (reader, writer) = std::io::pipe()
					.map_err(|source| tg::error!(!source, "failed to open a pipe"))?;
				let reader = tokio::net::unix::pipe::Receiver::from_owned_fd(reader.into())
					.map_err(|source| tg::error!(!source, "failed to convert the pipe fd"))?;
				(OutputStream::Pipe(reader), Some(writer.into()))
			},
			Stdio::Tty => {
				let pty = pty
					.as_ref()
					.ok_or_else(|| tg::error!("expected a tty arg"))?;
				let master = pty
					.master
					.try_clone()
					.map_err(|source| tg::error!(!source, "failed to clone the master fd"))?;
				let master = AsyncPtyFd::new(master)
					.map_err(|source| tg::error!(!source, "failed to create the async pty fd"))?;
				(OutputStream::Pty(master), None)
			},
		};

		let (host_stderr, guest_stderr): (OutputStream, Option<OwnedFd>) = match arg.command.stderr
		{
			Stdio::Null => {
				let fd = File::options()
					.write(true)
					.open("/dev/null")
					.map_err(|source| tg::error!(!source, "failed to open /dev/null"))?
					.into();
				(OutputStream::Null, Some(fd))
			},
			Stdio::Pipe => {
				let (reader, writer) = std::io::pipe()
					.map_err(|source| tg::error!(!source, "failed to open a pipe"))?;
				let reader = tokio::net::unix::pipe::Receiver::from_owned_fd(reader.into())
					.map_err(|source| tg::error!(!source, "failed to convert the pipe fd"))?;
				(OutputStream::Pipe(reader), Some(writer.into()))
			},
			Stdio::Tty => {
				let pty = pty
					.as_ref()
					.ok_or_else(|| tg::error!("expected a tty arg"))?;
				let master = pty
					.master
					.try_clone()
					.map_err(|source| tg::error!(!source, "failed to clone the master fd"))?;
				let master = AsyncPtyFd::new(master)
					.map_err(|source| tg::error!(!source, "failed to create the async pty fd"))?;
				(OutputStream::Pty(master), None)
			},
		};

		let id = arg.id.clone();
		let context = SpawnContext {
			command: arg.command,
			stdin: guest_stdin,
			stdout: guest_stdout,
			stderr: guest_stderr,
			pty: pty.as_ref().map(|pty| pty.name.clone()),
		};

		#[cfg(target_os = "linux")]
		let pid = crate::linux::spawn(context)?;

		#[cfg(target_os = "linux")]
		self.pids.insert(pid, id.clone());

		#[cfg(target_os = "macos")]
		let child = crate::darwin::spawn(context)?;

		#[cfg(target_os = "macos")]
		let pid = {
			use num::ToPrimitive as _;
			child
				.id()
				.and_then(|pid| pid.to_i32())
				.ok_or_else(|| tg::error!("failed to get the process id"))?
		};

		if let Some(pty) = &mut pty {
			pty.slave.take();
		}
		let pty = pty.map(Mutex::new);
		let process = Process {
			pid,
			#[cfg(target_os = "macos")]
			child: Arc::new(Mutex::new(Some(child))),
			status: None,
			notify: std::sync::Arc::new(tokio::sync::Notify::new()),
			stdin: Arc::new(Mutex::new(host_stdin)),
			stdout: Arc::new(Mutex::new(host_stdout)),
			stderr: Arc::new(Mutex::new(host_stderr)),
			pty: pty.map(Arc::new),
		};
		self.processes.insert(id.clone(), process);

		#[cfg(target_os = "macos")]
		{
			let child = self
				.processes
				.get(&id)
				.map(|process| process.child.clone())
				.ok_or_else(|| tg::error!(process = %id, "not found"))?;
			let server = self.clone();
			let process_id = id.clone();
			tokio::spawn(async move {
				let status = {
					let mut child = child.lock().await;
					let Some(mut child) = child.take() else {
						return;
					};
					child
						.wait()
						.await
						.map_err(|source| tg::error!(!source, "failed to wait for the process"))
						.map(exit_status_to_code)
				};
				if let Some(mut process) = server.processes.get_mut(&process_id) {
					process.status.replace(status);
					process.notify.notify_waiters();
				}
			});
		}

		Ok(crate::client::spawn::Output { id })
	}

	pub(crate) async fn handle_spawn_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;

		// Spawn.
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

#[cfg(target_os = "macos")]
fn exit_status_to_code(status: std::process::ExitStatus) -> u8 {
	use num::ToPrimitive as _;
	use std::os::unix::process::ExitStatusExt as _;
	status
		.code()
		.or(status.signal())
		.map_or(128, |code| code.to_u8().unwrap())
}
