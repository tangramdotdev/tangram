use {
	crate::Server,
	bytes::Bytes,
	futures::{Stream, TryStreamExt as _, future, stream},
	std::{
		collections::BTreeMap,
		path::{Path, PathBuf},
		pin::pin,
	},
	tangram_client as tg,
	tokio::io::{AsyncRead, AsyncReadExt as _},
};

/// Render a value.
pub fn render_value(artifacts_path: &Path, value: &tg::value::Data) -> String {
	if let Ok(string) = value.try_unwrap_string_ref() {
		return string.clone();
	}
	if let Ok(object) = value.try_unwrap_object_ref() {
		if let Ok(artifact) = tg::artifact::Id::try_from(object.clone()) {
			let string = artifacts_path
				.join(artifact.to_string())
				.to_str()
				.unwrap()
				.to_owned();
			return string;
		}
	}
	if let Ok(template) = value.try_unwrap_template_ref() {
		let string = template.render(|component| match component {
			tg::template::data::Component::String(string) => string.clone().into(),
			tg::template::data::Component::Artifact(artifact) => artifacts_path
				.join(artifact.to_string())
				.to_str()
				.unwrap()
				.to_owned()
				.into(),
		});
		return string;
	}
	"<tangram value>".to_owned()
}

pub fn render_env(
	artifacts_path: &Path,
	env: &tg::value::data::Map,
) -> tg::Result<BTreeMap<String, String>> {
	let mut output = BTreeMap::new();
	for (key, value) in env {
		let mutation = match value {
			tg::value::Data::Mutation(value) => value.clone(),
			value => tg::mutation::Data::Set {
				value: Box::new(value.clone()),
			},
		};
		mutation.apply(&mut output, key)?;
	}
	let output = output
		.iter()
		.map(|(key, value)| {
			let key = key.clone();
			let value = render_value(artifacts_path, value);
			Ok::<_, tg::Error>((key, value))
		})
		.collect::<tg::Result<_>>()?;
	Ok(output)
}

pub async fn stdio_task(
	server: &Server,
	process: &tg::Process,
	stdin: Option<tokio::process::ChildStdin>,
	stdout: Option<tokio::process::ChildStdout>,
	stderr: Option<tokio::process::ChildStderr>,
) -> tg::Result<()> {
	let state = process.load(server).await?;

	// Dump blobs to stdin if necessary.
	let stdin = tokio::spawn({
		let server = server.clone();
		let blob = state.command.load(&server).await?.stdin.clone();
		async move {
			let Some(mut stdin) = stdin else {
				return;
			};
			let Some(blob) = blob else {
				return;
			};
			// Copy blobs to stdin.
			let Ok(mut reader) = blob
				.read(&server, tg::blob::read::Arg::default())
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to read blob"))
			else {
				return;
			};
			tokio::io::copy(&mut reader, &mut stdin)
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to copy blob"))
				.ok();
		}
	});

	let stdout = tokio::spawn({
		let server = server.clone();
		let process = process.clone();
		async move {
			let Some(stdout) = stdout else { return Ok(()) };
			output(&server, &process, tg::process::log::Stream::Stdout, stdout)
				.await
				.inspect_err(|source| tracing::error!(?source, "failed to read stdout"))?;
			Ok::<_, tg::Error>(())
		}
	});

	let stderr = tokio::spawn({
		let server = server.clone();
		let process = process.clone();
		async move {
			let Some(stderr) = stderr else { return Ok(()) };
			output(&server, &process, tg::process::log::Stream::Stderr, stderr)
				.await
				.inspect_err(|source| tracing::error!(?source, "failed to read stderr"))?;
			Ok::<_, tg::Error>(())
		}
	});

	// Join the tasks.
	let (stdout, stderr) = future::join(stdout, stderr).await;
	stdin.abort();
	stdout
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to read stdout from pipe"))?;
	stderr
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to read stderr from pipe"))?;

	Ok::<_, tg::Error>(())
}

fn chunk_stream_from_reader(
	reader: impl AsyncRead + Unpin + Send + 'static,
) -> impl Stream<Item = tg::Result<Bytes>> + Send + 'static {
	let buffer = vec![0u8; 4096];
	stream::try_unfold((reader, buffer), |(mut reader, mut buffer)| async move {
		let size = reader
			.read(&mut buffer)
			.await
			.map_err(|source| tg::error!(!source, "failed to read"))?;
		if size == 0 {
			return Ok(None);
		}
		let chunk = Bytes::copy_from_slice(&buffer[0..size]);
		Ok(Some((chunk, (reader, buffer))))
	})
}

pub async fn signal_task(
	server: &Server,
	pid: libc::pid_t,
	process: &tg::Process,
) -> tg::Result<()> {
	// Get the signal stream for the process.
	let arg = tg::process::signal::get::Arg {
		remote: process.remote().cloned(),
	};
	let mut stream = server.try_get_process_signal_stream(process.id(), arg).await
		.map_err(|source| tg::error!(!source, %process = process.id(), "failed to get the process's signal stream"))?
		.ok_or_else(|| tg::error!(%process = process.id(), "expected the process's signal stream to exist"))?;

	// Handle the events.
	while let Some(event) = stream.try_next().await? {
		match event {
			tg::process::signal::get::Event::Signal(signal) => unsafe {
				let ret = libc::kill(pid, signal_number(signal));
				if ret != 0 {
					let error = std::io::Error::last_os_error();
					tracing::error!(?error, "failed to send the signal");
				}
			},
			tg::process::signal::get::Event::End => break,
		}
	}

	Ok(())
}

fn signal_number(signal: tg::process::Signal) -> i32 {
	match signal {
		tg::process::Signal::SIGABRT => libc::SIGABRT,
		tg::process::Signal::SIGFPE => libc::SIGFPE,
		tg::process::Signal::SIGILL => libc::SIGILL,
		tg::process::Signal::SIGALRM => libc::SIGALRM,
		tg::process::Signal::SIGHUP => libc::SIGHUP,
		tg::process::Signal::SIGINT => libc::SIGINT,
		tg::process::Signal::SIGKILL => libc::SIGKILL,
		tg::process::Signal::SIGPIPE => libc::SIGPIPE,
		tg::process::Signal::SIGQUIT => libc::SIGQUIT,
		tg::process::Signal::SIGSEGV => libc::SIGSEGV,
		tg::process::Signal::SIGTERM => libc::SIGTERM,
		tg::process::Signal::SIGUSR1 => libc::SIGUSR1,
		tg::process::Signal::SIGUSR2 => libc::SIGUSR2,
	}
}

async fn output(
	server: &Server,
	process: &tg::Process,
	stream: tg::process::log::Stream,
	reader: impl AsyncRead + Unpin + Send + 'static,
) -> tg::Result<()> {
	let chunks = chunk_stream_from_reader(reader);
	let mut chunks = pin!(chunks);
	while let Some(bytes) = chunks.try_next().await? {
		let arg = tg::process::log::post::Arg {
			bytes,
			stream,
			remote: process.remote().cloned(),
		};
		server.post_process_log(process.id(), arg).await?;
	}
	Ok(())
}

pub async fn which(exe: &Path, env: &BTreeMap<String, String>) -> tg::Result<PathBuf> {
	if exe.is_absolute() || exe.components().count() > 1 {
		return Ok(exe.to_owned());
	}
	let Some(pathenv) = env.get("PATH") else {
		return Ok(exe.to_owned());
	};
	let name = exe.components().next();
	let Some(std::path::Component::Normal(name)) = name else {
		return Err(tg::error!(%path = exe.display(), "invalid executable path"));
	};
	let sep = ":";
	for path in pathenv.split(sep) {
		let path = Path::new(path).join(name);
		if tokio::fs::try_exists(&path).await.ok() == Some(true) {
			return Ok(path);
		}
	}
	Err(tg::error!(%path = exe.display(), "failed to find the executable"))
}

pub async fn log(
	server: &Server,
	process: &tg::Process,
	stream: tg::process::log::Stream,
	message: String,
) {
	let state = process.load(server).await.unwrap();
	let stderr = state.stderr.as_ref();
	let stdout = state.stdout.as_ref();

	// If the pty or pipe is set, write to it.
	if let (tg::process::log::Stream::Stderr, Some(io)) = (stream, stderr) {
		server
			.write_message_to_stdio(io, message, process.remote())
			.await
			.ok();
		return;
	}
	if let (tg::process::log::Stream::Stdout, Some(io)) = (stream, stdout) {
		server
			.write_message_to_stdio(io, message, process.remote())
			.await
			.ok();
		return;
	}

	// Otherwise, post to the process's logs.
	let arg = tg::process::log::post::Arg {
		bytes: message.into(),
		remote: process.remote().cloned(),
		stream,
	};
	server
		.post_process_log(process.id(), arg)
		.await
		.inspect_err(|error| tracing::error!(?error, "failed to post process log"))
		.ok();
}

#[cfg(target_os = "linux")]
pub fn whoami() -> tg::Result<String> {
	unsafe {
		let uid = libc::getuid();
		let pwd = libc::getpwuid(uid);
		if pwd.is_null() {
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(!source, "failed to get username"));
		}
		let username = std::ffi::CString::from_raw((*pwd).pw_name)
			.to_str()
			.map_err(|source| tg::error!(!source, "non-utf8 username"))?
			.to_owned();
		Ok(username)
	}
}

impl Server {
	pub(super) async fn checkout_children(&self, process: &tg::Process) -> tg::Result<()> {
		// Do nothing if the VFS is enabled.
		if self.vfs.lock().unwrap().is_some() {
			return Ok(());
		}

		// Get the process's command's children that are artifacts.
		let artifacts: Vec<tg::artifact::Id> = process
			.command(self)
			.await?
			.children(self)
			.await?
			.into_iter()
			.filter_map(|object| object.id().try_into().ok())
			.collect::<Vec<_>>();

		// Check out the artifacts.
		let arg = tg::cache::Arg { artifacts };
		let stream = self.cache(arg).await?;

		// Log the progress stream.
		self.log_progress_stream(process, stream).await?;

		Ok(())
	}

	pub(super) async fn write_message_to_stdio(
		&self,
		stdio: &tg::process::Stdio,
		message: String,
		remote: Option<&String>,
	) -> tg::Result<()> {
		match stdio {
			tg::process::Stdio::Pipe(id) => {
				let bytes = Bytes::from(message);
				let stream = stream::once(future::ok(tg::pipe::Event::Chunk(bytes)));
				let arg = tg::pipe::write::Arg {
					remote: remote.cloned(),
				};
				self.write_pipe(id, arg, Box::new(stream)).await?;
			},
			tg::process::Stdio::Pty(id) => {
				let bytes = Bytes::from(message.replace('\n', "\r\n"));
				let stream = stream::once(future::ok(tg::pty::Event::Chunk(bytes)));
				let arg = tg::pty::write::Arg {
					master: false,
					remote: remote.cloned(),
				};
				self.write_pty(id, arg, Box::new(stream)).await?;
			},
		}
		Ok(())
	}
}
