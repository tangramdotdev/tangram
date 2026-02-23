use {
	crate::{Context, Server, temp::Temp},
	byteorder::ReadBytesExt as _,
	std::{
		os::{fd::AsRawFd as _, unix::process::ExitStatusExt as _},
		sync::Arc,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_sandbox as sandbox,
};

impl Server {
	pub(crate) async fn create_sandbox(
		&self,
		context: &Context,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Create the sandbox ID and temp directory.
		let id = tg::sandbox::Id::new();
		let temp = Temp::new(self);
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the temp directory"))?;

		// Create the output directory.
		tokio::fs::create_dir_all(temp.path().join("output"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the output directory"))?;

		let path = temp.path().join("socket");
		let (root, args) = self.get_sandbox_args(&id, &arg, &temp).await?;

		// Set up a pipe to notify the server once the child process has bound its server.
		let (mut reader, writer) = std::io::pipe()
			.map_err(|source| tg::error!(!source, "failed to create the ready pipe"))?;
		let flags = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETFD) };
		if flags < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to get the ready pipe flags"
			));
		}
		if unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0
		{
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to set the ready pipe flags"
			));
		}
		let ready_fd = writer.as_raw_fd();

		// Spawn the sandbox process.
		let executable = tangram_util::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the current executable"))?;
		let mut process = tokio::process::Command::new(executable)
			.kill_on_drop(true)
			.process_group(0)
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::inherit())
			.arg("sandbox")
			.arg("serve")
			.arg("--socket")
			.arg(&path)
			.arg("--ready-fd")
			.arg(ready_fd.to_string())
			.args(&args)
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the sandbox process"))?;
		drop(writer);

		// Wait for the sandbox process to signal readiness.
		let task = tokio::task::spawn_blocking(move || reader.read_u8());
		let result = tokio::time::timeout(Duration::from_secs(5), task)
			.await
			.map_err(|source| tg::error!(!source, "timed out waiting for the sandbox ready signal"))
			.and_then(|output| {
				output.map_err(|source| tg::error!(!source, "the sandbox ready task panicked"))
			})
			.and_then(|output| {
				output.map_err(|source| {
					tg::error!(!source, "failed to read the sandbox ready signal")
				})
			})
			.and_then(|byte| {
				if byte != 0x00 {
					return Err(tg::error!("received an invalid ready byte {byte}"));
				}
				Ok(())
			});
		if let Err(source) = result {
			process.start_kill().ok();
			let output = tokio::time::timeout(Duration::from_secs(1), process.wait_with_output())
				.await
				.ok()
				.and_then(Result::ok);
			let stderr = output
				.as_ref()
				.map(|output| String::from_utf8_lossy(&output.stderr));
			let exit_code = output.as_ref().and_then(|output| output.status.code());
			let signal = output
				.as_ref()
				.and_then(|output| output.status.stopped_signal());
			let error = if let Some(stderr) = stderr {
				tg::error!(
					!source,
					stderr = %stderr,
					exit_code = ?exit_code,
					signal = ?signal,
					"failed to start the sandbox"
				)
			} else {
				tg::error!(
					!source,
					exit_code = ?exit_code,
					signal = ?signal,
					"failed to start the sandbox"
				)
			};
			return Err(error);
		}

		// Connect to the sandbox.
		let client = sandbox::client::Client::connect(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the sandbox"))?;

		// Store the sandbox.
		self.sandboxes.insert(
			id.clone(),
			super::Sandbox {
				process,
				client: Arc::new(client),
				root,
				_temp: temp,
			},
		);

		Ok(tg::sandbox::create::Output { id })
	}

	pub(crate) async fn handle_create_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		let output = self
			.create_sandbox(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the sandbox"))?;

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
