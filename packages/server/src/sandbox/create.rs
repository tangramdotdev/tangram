use {
	crate::{Context, Server, temp::Temp},
	std::{
		future::Future,
		io::Read as _,
		os::{fd::AsRawFd as _, unix::process::ExitStatusExt as _},
		path::PathBuf,
		sync::Arc,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_sandbox as sandbox,
	tangram_uri::Uri,
};

const MAX_URL_LEN: usize = 100;

impl Server {
	pub(crate) fn create_sandbox_with_context<'a>(
		&'a self,
		context: &'a Context,
		arg: tg::sandbox::create::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::create::Output>> + Send + 'a {
		self.create_sandbox_with_context_inner(context, arg)
	}

	async fn create_sandbox_with_context_inner<'a>(
		&'a self,
		context: &'a Context,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		if context.sandbox.is_some() {
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

		let path;
		#[cfg(target_os = "linux")]
		{
			path = temp.path().join("socket");
		}
		#[cfg(target_os = "macos")]
		{
			path = std::path::Path::new("/tmp").join(format!("{id}.socket"));
		}

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
		let mut command = tokio::process::Command::new(executable);
		command
			.kill_on_drop(true)
			.process_group(0)
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::inherit())
			.arg("sandbox")
			.arg("run")
			.arg("--id")
			.arg(id.to_string())
			.arg("--path")
			.arg(temp.path())
			.arg("--server-path")
			.arg(&self.path)
			.arg("--ready-fd")
			.arg(ready_fd.to_string())
			.arg("--socket")
			.arg(&path);
		for mount in &arg.mounts {
			command.arg("--mount");
			command.arg(mount.to_string());
		}
		if arg.network {
			command.arg("--network");
		} else {
			command.arg("--no-network");
		}
		if let Some(hostname) = &arg.hostname {
			command.arg("--hostname");
			command.arg(hostname);
		}
		if let Some(user) = &arg.user {
			command.arg("--user");
			command.arg(user);
		}
		let mut process = command
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the sandbox process"))?;
		drop(writer);

		// Wait for the sandbox process to signal readiness.
		let task = tokio::task::spawn_blocking(move || {
			let mut bytes = Vec::new();
			reader.read_to_end(&mut bytes).map(|_| bytes)
		});
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
			.and_then(|bytes| {
				serde_json::from_slice::<sandbox::Ready>(&bytes).map_err(|source| {
					tg::error!(!source, "failed to deserialize the sandbox ready payload")
				})
			})
			.map(|ready| PathBuf::from(ready.root_path));
		let root = match result {
			Ok(root) => root,
			Err(source) => {
				process.start_kill().ok();
				let output =
					tokio::time::timeout(Duration::from_secs(1), process.wait_with_output())
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
			},
		};

		// Connect to the sandbox.
		let client = sandbox::client::Client::connect(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the sandbox"))?;

		// Create the proxy server for this sandbox.
		let socket_path = temp.path().join(".tangram/socket");
		tokio::fs::create_dir_all(socket_path.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the proxy socket directory"))?;
		let host_socket = socket_path
			.to_str()
			.ok_or_else(|| tg::error!("the proxy socket path is not valid UTF-8"))?;
		let url = if cfg!(target_os = "linux") || host_socket.len() <= MAX_URL_LEN {
			tangram_uri::Uri::builder()
				.scheme("http+unix")
				.authority(host_socket)
				.path("")
				.build()
				.unwrap()
		} else {
			"http://localhost:0".to_owned().parse::<Uri>().unwrap()
		};
		let listener = Server::listen(&url)
			.await
			.map_err(|source| tg::error!(!source, "failed to listen on the proxy socket"))?;
		let listener_addr = listener
			.local_addr()
			.map_err(|source| tg::error!(!source, "failed to get listener address"))?;

		let proxy_url = if let tokio_util::either::Either::Right(listener) = listener_addr {
			let port = listener.port();
			Some(format!("http://localhost:{port}").parse::<Uri>().unwrap())
		} else {
			None
		};
		let paths = if cfg!(target_os = "linux") {
			Some(crate::context::Paths {
				server_host: self.path.clone(),
				output_host: temp.path().join("output"),
				output_guest: "/output".into(),
				root_host: root.clone(),
			})
		} else {
			None
		};
		let context = Context {
			sandbox: Some(Arc::new(crate::context::Sandbox {
				id: id.clone(),
				paths,
				remote: None,
				retry: false,
			})),
			..Default::default()
		};
		let serve_task = {
			let server = self.clone();
			let context = context.clone();
			Task::spawn(|stop| async move {
				server.serve(listener, context, stop).await;
			})
		};

		// Take the stderr handle from the child process.
		let stderr = process.stderr.take();

		// Store the sandbox.
		self.sandboxes.insert(
			id.clone(),
			super::Sandbox {
				process,
				client: Arc::new(client),
				context,
				refcount: 0,
				serve_task,
				stderr,
				temp,
				proxy_url,
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
			.create_sandbox_with_context(context, arg)
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
