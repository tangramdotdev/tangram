use {
	crate::prelude::*,
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future},
	num::ToPrimitive as _,
	serde_with::serde_as,
	std::collections::BTreeMap,
	tangram_futures::task::Task,
	tangram_util::{io, serde::BytesBase64},
	tokio::io::AsyncWriteExt as _,
};

mod reader;
#[cfg(test)]
mod tests;
mod writer;

pub use self::{reader::Reader, writer::Writer};

pub mod read;
pub mod write;

pub const MAX_CHUNK_SIZE: usize = 32 * 1024;

pub const TANGRAM_CONTENT_TYPE: &str = "application/vnd.tangram.process-stdio";

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	derive_more::IsVariant,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub enum Stdio {
	Blob(Box<tg::Referent<tg::blob::Id>>),
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
	derive_more::Display,
	derive_more::FromStr,
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
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum Stream {
	#[tangram_serialize(id = 0)]
	Stdin,

	#[tangram_serialize(id = 1)]
	Stdout,

	#[tangram_serialize(id = 2)]
	Stderr,
}

#[serde_as]
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Chunk {
	#[serde_as(as = "BytesBase64")]
	#[tangram_serialize(id = 0)]
	pub bytes: Bytes,

	#[tangram_serialize(id = 1)]
	pub combined_position: u64,

	#[tangram_serialize(id = 2)]
	pub stream: Stream,

	#[tangram_serialize(id = 3)]
	pub stream_position: u64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "Option::is_none")]
	pub timestamp: Option<i64>,
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct End {
	#[tangram_serialize(id = 0)]
	pub combined_position: u64,
	#[tangram_serialize(id = 1)]
	pub stream_positions: BTreeMap<Stream, u64>,
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
				.map(|referent| Self::Blob(Box::new(referent)))
				.map_err(|_| tg::error!(%value, "invalid stdio")),
		}
	}
}

/// The arg of a task that connects a process's stdio to this process's stdio, which locates the process by its attach's nonce.
pub(super) struct StdioTaskArg<I> {
	pub instance: I,
	pub location: Option<tg::location::Arg>,
	pub nonce: tg::process::Nonce,
	pub raw: bool,
	pub stderr: Option<tg::process::Stdio>,
	pub stdin: Option<tg::process::Stdio>,
	pub stdout: Option<tg::process::Stdio>,
	pub tty: bool,
}

pub(super) async fn stdio_task<I>(arg: StdioTaskArg<I>) -> tg::Result<()>
where
	I: tg::Instance,
{
	let StdioTaskArg {
		instance,
		location,
		nonce,
		raw,
		stderr,
		stdin,
		stdout,
		tty,
	} = arg;
	let mut stdin_task = stdin.map(|stdin| {
		let instance = instance.clone();
		let location = location.clone();
		Task::spawn(
			move |_| async move { stdin_task(&instance, location, nonce, stdin, raw).await },
		)
	});

	let sigwinch_task = if tty {
		let instance = instance.clone();
		let location = location.clone();
		let task = Task::spawn(|_| async move { sigwinch_task(&instance, location, nonce).await });
		Some(task)
	} else {
		None
	};

	let output = if stdout.is_some() || stderr.is_some() {
		stdout_stderr_task(&instance, location, nonce, stdout, stderr).await
	} else {
		Ok(())
	};

	let stdin = if let Some(task) = stdin_task.take() {
		task.abort();
		match task.wait().await {
			Ok(result) => result,
			Err(error) if error.is_cancelled() => Ok(()),
			Err(error) => Err(tg::error!(!error, "the stdin task panicked")),
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

async fn stdin_task<I>(
	instance: &I,
	location: Option<tg::location::Arg>,
	nonce: tg::process::Nonce,
	stdin: tg::process::Stdio,
	raw: bool,
) -> tg::Result<()>
where
	I: tg::Instance,
{
	if !matches!(stdin, tg::process::Stdio::Pipe | tg::process::Stdio::Tty) {
		return Ok(());
	}
	#[cfg(unix)]
	let _raw_mode_guard =
		if raw && tangram_util::tty::is_foreground_controlling_tty(libc::STDIN_FILENO) {
			let fd = libc::STDIN_FILENO;
			let mut original = std::mem::MaybeUninit::<libc::termios>::uninit();
			if unsafe { libc::tcgetattr(fd, original.as_mut_ptr()) } != 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to get stdin termios"
				));
			}
			let original = unsafe { original.assume_init() };
			let mut raw = original;
			unsafe {
				libc::cfmakeraw(std::ptr::addr_of_mut!(raw));
			}
			if unsafe { libc::tcsetattr(fd, libc::TCSAFLUSH, std::ptr::addr_of!(raw)) } != 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to set stdin raw mode"
				));
			}
			Some(scopeguard::guard(original, move |original| unsafe {
				libc::tcsetattr(fd, libc::TCSAFLUSH, std::ptr::addr_of!(original));
			}))
		} else {
			None
		};
	#[cfg(not(unix))]
	let _ = raw;
	let arg = tg::process::stdio::write::stream::Arg {
		id: None,
		location,
		streams: vec![tg::process::stdio::Stream::Stdin],
		tokens: tg::authorization::Tokens::default(),
	};
	let input = io::stdin()
		.map_err(|error| tg::error!(!error, "failed to open stdin"))?
		.filter_map(|result| {
			future::ready(match result {
				Ok(bytes) if bytes.is_empty() => None,
				Ok(bytes) => Some(Ok(bytes)),
				Err(error) => Some(Err(tg::error!(!error, "failed to read stdin"))),
			})
		})
		.scan(0_u64, |position, result| {
			let result = result.and_then(|bytes| {
				let length = bytes.len().to_u64().unwrap();
				let chunk = tg::process::stdio::Chunk {
					bytes,
					combined_position: *position,
					stream: tg::process::stdio::Stream::Stdin,
					stream_position: *position,
					timestamp: None,
				};
				*position = position
					.checked_add(length)
					.ok_or_else(|| tg::error!("the stdin position is too large"))?;

				Ok(chunk)
			});

			future::ready(Some(result))
		})
		.boxed();
	let process = tg::Either::Left(nonce);
	instance.write_process_stdio_all(&process, arg, input).await
}

async fn stdout_stderr_task<I>(
	instance: &I,
	location: Option<tg::location::Arg>,
	nonce: tg::process::Nonce,
	stdout: Option<tg::process::Stdio>,
	stderr: Option<tg::process::Stdio>,
) -> tg::Result<()>
where
	I: tg::Instance,
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
		id: None,
		length: None,
		location,
		position: None,
		size: None,
		streams,
		timeout: None,
		tokens: tg::authorization::Tokens::default(),
	};
	let process = tg::Either::Left(nonce);
	let Some(stream) = instance.try_read_process_stdio_all(&process, arg).await? else {
		return Ok(());
	};
	let mut stdout_writer = tokio::io::BufWriter::new(tokio::io::stdout());
	let mut writer = tokio::io::BufWriter::new(tokio::io::stderr());
	let mut stream = std::pin::pin!(stream);
	while let Some(chunk) = stream.try_next().await? {
		match chunk.stream {
			tg::process::stdio::Stream::Stdout
				if matches!(
					stdout,
					Some(tg::process::Stdio::Pipe | tg::process::Stdio::Tty)
				) =>
			{
				stdout_writer
					.write_all(&chunk.bytes)
					.await
					.map_err(|error| tg::error!(!error, "failed to write stdout"))?;
				stdout_writer
					.flush()
					.await
					.map_err(|error| tg::error!(!error, "failed to flush stdout"))?;
			},
			tg::process::stdio::Stream::Stderr if stderr.is_some() => {
				writer
					.write_all(&chunk.bytes)
					.await
					.map_err(|error| tg::error!(!error, "failed to write stderr"))?;
				writer
					.flush()
					.await
					.map_err(|error| tg::error!(!error, "failed to flush stderr"))?;
			},
			_ => (),
		}
	}
	Ok(())
}

async fn sigwinch_task<I>(
	instance: &I,
	location: Option<tg::location::Arg>,
	nonce: tg::process::Nonce,
) -> tg::Result<()>
where
	I: tg::Instance,
{
	let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())
		.map_err(|error| tg::error!(!error, "failed to create signal handler"))?;
	let process = tg::Either::Left(nonce);
	while let Some(()) = signal.recv().await {
		let Some(size) =
			tangram_util::tty::get_controlling_tty_size().map(|size| tg::process::tty::Size {
				rows: size.rows,
				cols: size.cols,
			})
		else {
			continue;
		};
		let arg = tg::process::tty::size::put::Arg {
			location: location.clone(),
			size,
			tokens: tg::authorization::Tokens::default(),
		};
		instance
			.set_process_tty_size(&process, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the tty"))?;
	}
	Ok(())
}
