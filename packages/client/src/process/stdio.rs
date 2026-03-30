use {
	crate::prelude::*,
	bytes::Bytes,
	futures::{
		StreamExt as _, TryStreamExt as _, future,
		stream::{self},
	},
	serde_with::serde_as,
	tangram_futures::task::Task,
	tangram_util::{io, serde::BytesBase64},
	tokio::io::AsyncWriteExt as _,
};

pub mod read;
pub mod write;

#[derive(
	Clone,
	Debug,
	Default,
	PartialEq,
	Eq,
	derive_more::IsVariant,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub enum Stdio {
	Blob(tg::blob::Id),
	#[default]
	Inherit,
	Log,
	Null,
	Pipe,
	Tty,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Stream {
	#[tangram_serialize(id = 0)]
	Stdin,

	#[tangram_serialize(id = 1)]
	Stdout,

	#[tangram_serialize(id = 2)]
	Stderr,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Chunk {
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub position: Option<u64>,

	pub stream: Stream,
}

impl std::fmt::Display for Stdio {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Blob(blob) => write!(f, "{blob}"),
			Self::Inherit => write!(f, "inherit"),
			Self::Log => write!(f, "log"),
			Self::Null => write!(f, "null"),
			Self::Pipe => write!(f, "pipe"),
			Self::Tty => write!(f, "tty"),
		}
	}
}

impl std::str::FromStr for Stdio {
	type Err = tg::Error;

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		match value {
			"inherit" => Ok(Self::Inherit),
			"log" => Ok(Self::Log),
			"null" => Ok(Self::Null),
			"pipe" => Ok(Self::Pipe),
			"tty" => Ok(Self::Tty),
			_ => value
				.parse()
				.map(Self::Blob)
				.map_err(|_| tg::error!(%value, "invalid stdio")),
		}
	}
}

impl std::fmt::Display for Stream {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Stdin => write!(f, "stdin"),
			Self::Stdout => write!(f, "stdout"),
			Self::Stderr => write!(f, "stderr"),
		}
	}
}

impl std::str::FromStr for Stream {
	type Err = tg::Error;

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		match value {
			"stdin" => Ok(Self::Stdin),
			"stdout" => Ok(Self::Stdout),
			"stderr" => Ok(Self::Stderr),
			_ => Err(tg::error!(%value, "invalid stream")),
		}
	}
}

pub(super) async fn stdio_task<H>(
	handle: H,
	id: tg::process::Id,
	remote: Option<String>,
	stdin: Option<tg::process::Stdio>,
	stdout: Option<tg::process::Stdio>,
	stderr: Option<tg::process::Stdio>,
	tty: bool,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	let mut stdin_task = stdin.map(|stdin| {
		let handle = handle.clone();
		let id = id.clone();
		let remote = remote.clone();
		Task::spawn(move |_| async move { stdin_task(&handle, id, remote, stdin).await })
	});

	let sigwinch_task = if tty {
		let handle = handle.clone();
		let id = id.clone();
		let remote = remote.clone();
		let task = Task::spawn(|_| async move { sigwinch_task(&handle, id, remote).await });
		Some(task)
	} else {
		None
	};

	let output = if stdout.is_some() || stderr.is_some() {
		let handle = handle.clone();
		let id = id.clone();
		let remote = remote.clone();
		stdout_stderr_task_task(&handle, id, remote, stdout, stderr).await
	} else {
		Ok(())
	};

	let stdin = if let Some(task) = stdin_task.take() {
		task.abort();
		match task.wait().await {
			Ok(result) => result,
			Err(error) if error.is_cancelled() => Ok(()),
			Err(source) => Err(tg::error!(!source, "the stdin task panicked")),
		}
	} else {
		Ok(())
	};

	if let Some(task) = sigwinch_task {
		task.abort();
	}

	stdin.and(output)?;

	Ok(())
}

async fn stdin_task<H>(
	handle: &H,
	id: tg::process::Id,
	remote: Option<String>,
	stdin: tg::process::Stdio,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	if !matches!(stdin, tg::process::Stdio::Pipe | tg::process::Stdio::Tty) {
		return Ok(());
	}
	let arg = tg::process::stdio::write::Arg {
		streams: vec![tg::process::stdio::Stream::Stdin],
		remotes: remote.map(|remote| vec![remote]),
		..Default::default()
	};
	let input = io::stdin()
		.map_err(|source| tg::error!(!source, "failed to open stdin"))?
		.filter_map(|result| {
			future::ready(match result {
				Ok(bytes) if bytes.is_empty() => None,
				Ok(bytes) => Some(Ok(tg::process::stdio::read::Event::Chunk(
					tg::process::stdio::Chunk {
						bytes,
						position: None,
						stream: tg::process::stdio::Stream::Stdin,
					},
				))),
				Err(error) => Some(Err(tg::error!(!error, "failed to read stdin"))),
			})
		})
		.chain(stream::once(future::ok(
			tg::process::stdio::read::Event::End,
		)))
		.boxed();
	let output = handle.write_process_stdio(&id, arg, input).await?;
	let mut output = std::pin::pin!(output);
	while let Some(event) = output.try_next().await? {
		match event {
			tg::process::stdio::write::Event::End => break,
			tg::process::stdio::write::Event::Stop => (),
		}
	}
	Ok(())
}

async fn stdout_stderr_task_task<H>(
	handle: &H,
	id: tg::process::Id,
	remote: Option<String>,
	stdout: Option<tg::process::Stdio>,
	stderr: Option<tg::process::Stdio>,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	let stdout = stdout
		.filter(|stdout| matches!(stdout, tg::process::Stdio::Pipe | tg::process::Stdio::Tty));
	let stderr = stderr
		.filter(|stderr| matches!(stderr, tg::process::Stdio::Pipe | tg::process::Stdio::Tty));
	let streams = [
		stdout.as_ref().map(|_| tg::process::stdio::Stream::Stdout),
		stderr.as_ref().map(|_| tg::process::stdio::Stream::Stderr),
	]
	.into_iter()
	.flatten()
	.collect::<Vec<_>>();
	if streams.is_empty() {
		return Ok(());
	}
	let arg = tg::process::stdio::read::Arg {
		streams,
		remotes: remote.map(|remote| vec![remote]),
		..Default::default()
	};
	let Some(stream) = handle.try_read_process_stdio_all(&id, arg).await? else {
		return Ok(());
	};
	let mut stdout_writer = tokio::io::BufWriter::new(tokio::io::stdout());
	let mut writer = tokio::io::BufWriter::new(tokio::io::stderr());
	let mut stream = std::pin::pin!(stream);
	while let Some(event) = stream.try_next().await? {
		match event {
			tg::process::stdio::read::Event::Chunk(chunk) => match chunk.stream {
				tg::process::stdio::Stream::Stdout
					if matches!(
						stdout,
						Some(tg::process::Stdio::Pipe | tg::process::Stdio::Tty)
					) =>
				{
					stdout_writer
						.write_all(&chunk.bytes)
						.await
						.map_err(|source| tg::error!(!source, "failed to write stdout"))?;
					stdout_writer
						.flush()
						.await
						.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;
				},
				tg::process::stdio::Stream::Stderr if stderr.is_some() => {
					writer
						.write_all(&chunk.bytes)
						.await
						.map_err(|source| tg::error!(!source, "failed to write stderr"))?;
					writer
						.flush()
						.await
						.map_err(|source| tg::error!(!source, "failed to flush stderr"))?;
				},
				_ => (),
			},
			tg::process::stdio::read::Event::End => break,
		}
	}
	Ok(())
}

async fn sigwinch_task<H>(handle: &H, id: tg::process::Id, remote: Option<String>) -> tg::Result<()>
where
	H: tg::Handle,
{
	let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())
		.map_err(|source| tg::error!(!source, "failed to create signal handler"))?;
	while let Some(()) = signal.recv().await {
		let arg = tg::process::tty::size::put::Arg {
			local: None,
			remotes: remote.clone().map(|remote| vec![remote]),
			size: get_tty_size().ok_or_else(|| tg::error!("failed to get the tty size"))?,
		};
		handle
			.set_process_tty_size(&id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the tty"))?;
	}
	Ok(())
}

pub(super) fn get_tty_size() -> Option<tg::process::tty::Size> {
	let tty = std::fs::OpenOptions::new()
		.read(true)
		.write(true)
		.open("/dev/tty")
		.ok();
	let tty_fd = tty.as_ref().map(std::os::fd::AsRawFd::as_raw_fd);
	if let Some(fd) = tty_fd {
		let mut size = unsafe { std::mem::zeroed::<libc::winsize>() };
		if unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, &mut size) } < 0
			|| size.ws_col == 0
			|| size.ws_row == 0
		{
			None
		} else {
			Some(tg::process::tty::Size {
				rows: size.ws_row,
				cols: size.ws_col,
			})
		}
	} else {
		None
	}
}
