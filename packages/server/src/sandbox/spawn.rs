use {
	crate::{Context, Server},
	std::os::fd::{AsFd as _, AsRawFd as _},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_sandbox as sandbox,
};

impl Server {
	pub(crate) async fn sandbox_spawn(
		&self,
		context: &Context,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::spawn::Arg,
	) -> tg::Result<tg::sandbox::spawn::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		let sandbox = self
			.sandboxes
			.get(id)
			.ok_or_else(|| tg::error!("sandbox not found"))?;
		let client = sandbox.client.clone();
		let root = sandbox.root.clone();
		drop(sandbox);

		// Collect FDs that need to be kept alive until after the spawn call.
		let mut fds = Vec::new();

		// Handle stdin.
		let stdin = match &arg.stdin {
			tg::process::Stdio::Pipe(pipe_id) => {
				let pipe = self
					.pipes
					.get(pipe_id)
					.ok_or_else(|| tg::error!("failed to find the pipe"))?;
				let fd = pipe
					.receiver
					.as_fd()
					.try_clone_to_owned()
					.map_err(|source| tg::error!(!source, "failed to clone the receiver"))?;
				let receiver = tokio::net::unix::pipe::Receiver::from_owned_fd_unchecked(fd)
					.map_err(|source| tg::error!(!source, "io error"))?;
				let fd = receiver
					.into_blocking_fd()
					.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
				let raw_fd = fd.as_raw_fd();
				fds.push(fd);
				raw_fd
			},
			tg::process::Stdio::Pty(pty_id) => {
				let pty = self
					.ptys
					.get(pty_id)
					.ok_or_else(|| tg::error!("failed to find the pty"))?;
				let slave = pty
					.slave
					.as_ref()
					.ok_or_else(|| tg::error!("the pty slave is closed"))?;
				let fd = slave
					.try_clone()
					.map_err(|source| tg::error!(!source, "failed to clone the pty slave"))?;
				let raw_fd = fd.as_raw_fd();
				fds.push(fd);
				raw_fd
			},
		};

		// Handle stdout.
		let stdout = match &arg.stdout {
			tg::process::Stdio::Pipe(pipe_id) => {
				let pipe = self
					.pipes
					.get(pipe_id)
					.ok_or_else(|| tg::error!("failed to find the pipe"))?;
				let fd = pipe
					.sender
					.as_ref()
					.ok_or_else(|| tg::error!("the pipe is closed"))?
					.as_fd()
					.try_clone_to_owned()
					.map_err(|source| tg::error!(!source, "failed to clone the sender"))?;
				let sender = tokio::net::unix::pipe::Sender::from_owned_fd_unchecked(fd)
					.map_err(|source| tg::error!(!source, "io error"))?;
				let fd = sender
					.into_blocking_fd()
					.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
				let raw_fd = fd.as_raw_fd();
				fds.push(fd);
				raw_fd
			},
			tg::process::Stdio::Pty(pty_id) => {
				let pty = self
					.ptys
					.get(pty_id)
					.ok_or_else(|| tg::error!("failed to find the pty"))?;
				let slave = pty
					.slave
					.as_ref()
					.ok_or_else(|| tg::error!("the pty slave is closed"))?;
				let fd = slave
					.try_clone()
					.map_err(|source| tg::error!(!source, "failed to clone the pty slave"))?;
				let raw_fd = fd.as_raw_fd();
				fds.push(fd);
				raw_fd
			},
		};

		// Handle stderr.
		let stderr = match &arg.stderr {
			tg::process::Stdio::Pipe(pipe_id) => {
				let pipe = self
					.pipes
					.get(pipe_id)
					.ok_or_else(|| tg::error!("failed to find the pipe"))?;
				let fd = pipe
					.sender
					.as_ref()
					.ok_or_else(|| tg::error!("the pipe is closed"))?
					.as_fd()
					.try_clone_to_owned()
					.map_err(|source| tg::error!(!source, "failed to clone the sender"))?;
				let sender = tokio::net::unix::pipe::Sender::from_owned_fd_unchecked(fd)
					.map_err(|source| tg::error!(!source, "io error"))?;
				let fd = sender
					.into_blocking_fd()
					.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
				let raw_fd = fd.as_raw_fd();
				fds.push(fd);
				raw_fd
			},
			tg::process::Stdio::Pty(pty_id) => {
				let pty = self
					.ptys
					.get(pty_id)
					.ok_or_else(|| tg::error!("failed to find the pty"))?;
				let slave = pty
					.slave
					.as_ref()
					.ok_or_else(|| tg::error!("the pty slave is closed"))?;
				let fd = slave
					.try_clone()
					.map_err(|source| tg::error!(!source, "failed to clone the pty slave"))?;
				let raw_fd = fd.as_raw_fd();
				fds.push(fd);
				raw_fd
			},
		};

		// Create the command.
		let command = sandbox::Command {
			chroot: Some(root),
			cwd: None,
			env: Vec::new(),
			executable: arg.command,
			hostname: None,
			mounts: Vec::new(),
			network: false,
			stdin: Some(stdin),
			stdout: Some(stdout),
			stderr: Some(stderr),
			trailing: arg.args,
			user: None,
		};

		// Spawn the command via the sandbox client.
		let pid = client
			.spawn(command)
			.await
			.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

		// Drop the FDs now that the spawn has completed.
		drop(fds);

		Ok(tg::sandbox::spawn::Output { pid })
	}


	pub(crate) async fn handle_sandbox_spawn_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the sandbox ID.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the sandbox id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Spawn the command.
		let output = self
			.sandbox_spawn(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to spawn the command in the sandbox"))?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();
		Ok(response)
	}
}
