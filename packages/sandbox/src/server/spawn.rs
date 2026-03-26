use {
	crate::{
		Stdio,
		common::{AsyncPtyFd, InputStream, OutputStream, Pty, SpawnContext},
		server::{ChildProcess, ChildStdio, Server},
	},
	std::{fs::File, os::fd::OwnedFd},
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

		let child_process = ChildProcess {
			#[cfg(target_os = "linux")]
			pid,
			#[cfg(target_os = "macos")]
			child,
			status: None,
			#[cfg(target_os = "linux")]
			notify: std::sync::Arc::new(tokio::sync::Notify::new()),
		};
		if let Some(pty) = &mut pty {
			pty.slave.take();
		}
		let child_stdio = ChildStdio {
			stdin: Mutex::new(host_stdin),
			stdout: Mutex::new(host_stdout),
			stderr: Mutex::new(host_stderr),
			pty: pty.map(Mutex::new),
		};
		self.stdio.insert(id.clone(), child_stdio);
		self.processes.insert(id.clone(), child_process);

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
