use crate::Server;
use bytes::Bytes;
use futures::{Stream, TryStreamExt as _, future, stream};
use std::{
	collections::BTreeMap,
	os::fd::OwnedFd,
	path::{Path, PathBuf},
	pin::pin,
};
use tangram_client as tg;
use tangram_sandbox as sandbox;
use tokio::io::{AsyncRead, AsyncReadExt as _};

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
			tg::template::component::Data::String(string) => string.clone().into(),
			tg::template::component::Data::Artifact(artifact) => artifacts_path
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
	stdin: Option<sandbox::ChildStdin>,
	stdout: Option<sandbox::ChildStdout>,
	stderr: Option<sandbox::ChildStderr>,
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

// Helper to create an event stream from pipes
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
	let is_pty = match stream {
		tg::process::log::Stream::Stderr => {
			matches!(
				process.load(server).await.unwrap().stderr,
				Some(tg::process::Stdio::Pty(_))
			)
		},
		tg::process::log::Stream::Stdout => {
			matches!(
				process.load(server).await.unwrap().stdout,
				Some(tg::process::Stdio::Pty(_))
			)
		},
	};
	let message = if is_pty {
		message.replace('\n', "\r\n")
	} else {
		message
	};
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

impl Server {
	pub(super) fn get_pty_or_pipe_fd(&self, io: &tg::process::Stdio) -> tg::Result<OwnedFd> {
		match io {
			tg::process::Stdio::Pipe(pipe) => Ok(self
				.pipes
				.get(pipe)
				.ok_or_else(|| tg::error!("failed to get pipe"))?
				.guest
				.try_clone()
				.map_err(|source| tg::error!(!source, "failed to get pipe"))?
				.into()),
			tg::process::Stdio::Pty(pty) => {
				let pty = self
					.ptys
					.get(pty)
					.ok_or_else(|| tg::error!("failed to get pty"))?;
				pty.guest
					.try_clone()
					.map_err(|source| tg::error!(!source, "failed to get pty"))
			},
		}
	}
}
