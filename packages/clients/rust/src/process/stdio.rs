use {
	crate::prelude::*,
	bytes::Bytes,
	futures::{
		StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	num::ToPrimitive as _,
	serde_with::serde_as,
	tangram_futures::{read::Ext as _, stream::Ext as _, task::Task, write::Ext as _},
	tangram_http::body::{BodyStream, Boxed},
	tangram_util::{io, serde::BytesBase64},
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::io::StreamReader,
};

mod reader;
mod writer;

pub use self::{reader::Reader, writer::Writer};

pub mod read;
pub mod write;

pub const SSE_CONTENT_TYPE: &str = "text/event-stream";
pub const TANGRAM_CONTENT_TYPE: &str = "application/vnd.tangram.process-stdio";

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

pub(crate) fn encode<T>(stream: BoxStream<'static, tg::Result<T>>, max_frame_size: u64) -> Boxed
where
	T: Send + tangram_serialize::Serialize + 'static,
{
	let stream = stream.then(move |result| async move {
		let frame = match result {
			Ok(message) => {
				let message = tangram_serialize::to_vec(&message)
					.map_err(|error| tg::error!(!error, "failed to serialize the stdio message"))?;
				let message_length = message.len();
				let length = u64::try_from(message_length).map_err(
					|error| tg::error!(!error, length = %message_length, "stdio frame length out of range"),
				)?;
				if length > max_frame_size {
					return Err(tg::error!(
						length = %length,
						max = %max_frame_size,
						"stdio frame too large"
					));
				}
				let mut bytes = Vec::with_capacity(9 + message.len());
				bytes.write_uvarint(length).await.unwrap();
				bytes.write_all(&message).await.unwrap();
				hyper::body::Frame::data(bytes.into())
			},
			Err(error) => {
				let mut trailers = http::HeaderMap::new();
				trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
				let json = error.state().object().map_or_else(
					|| serde_json::to_string(&error.id()).unwrap(),
					|object| serde_json::to_string(&object.to_data()).unwrap(),
				);
				trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
				hyper::body::Frame::trailers(trailers)
			},
		};
		Ok::<_, tg::Error>(frame)
	});

	Boxed::with_stream(stream)
}

pub(crate) fn decode<T>(body: Boxed, max_frame_size: u64) -> BoxStream<'static, tg::Result<T>>
where
	T: for<'de> tangram_serialize::Deserialize<'de> + Send + 'static,
{
	let mut stream = BodyStream::new(body);
	let (data_sender, data_receiver) = tokio::sync::mpsc::channel::<tg::Result<Bytes>>(1);
	let (trailer_sender, trailer_receiver) = tokio::sync::mpsc::channel(1);
	let task = Task::spawn(|_| async move {
		while let Some(result) = stream.next().await {
			match result {
				Ok(frame) if frame.is_data() => {
					let data = frame.into_data().unwrap();
					data_sender.send(Ok(data)).await.ok();
				},
				Ok(frame) if frame.is_trailers() => {
					let trailers = frame.into_trailers().unwrap();
					trailer_sender.send(trailers).await.ok();
				},
				Ok(_) => unreachable!(),
				Err(_) => break,
			}
		}
	});
	let reader =
		StreamReader::new(ReceiverStream::new(data_receiver).map_err(std::io::Error::other));
	let data_messages = stream::try_unfold(reader, move |mut reader| async move {
		let length = match reader.try_read_uvarint().await {
			Ok(Some(length)) => length,
			Ok(None) => return Ok(None),
			Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
			Err(error) => {
				return Err(tg::error!(!error, "failed to read the stdio frame length"));
			},
		};
		if length > max_frame_size {
			return Err(tg::error!(
				length = %length,
				max = %max_frame_size,
				"stdio frame too large"
			));
		}
		let length = usize::try_from(length).map_err(
			|error| tg::error!(!error, length = %length, "stdio frame length out of range"),
		)?;
		let mut bytes = vec![0; length];
		if let Err(error) = reader.read_exact(&mut bytes).await {
			if error.kind() == std::io::ErrorKind::UnexpectedEof {
				return Ok(None);
			}
			return Err(tg::error!(!error, "failed to read the stdio message"));
		}
		let message = tangram_serialize::from_slice(&bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the stdio message"))?;

		Ok(Some((message, reader)))
	});
	let trailer_messages = ReceiverStream::new(trailer_receiver).then(|trailers| async move {
		let event = trailers
			.get("x-tg-event")
			.ok_or_else(|| tg::error!("missing event"))?
			.to_str()
			.map_err(|error| tg::error!(!error, "invalid event"))?;
		if event != "error" {
			return Err(tg::error!("invalid event"));
		}
		let data = trailers
			.get("x-tg-data")
			.ok_or_else(|| tg::error!("missing data"))?
			.to_str()
			.map_err(|error| tg::error!(!error, "invalid data"))?;
		let error = serde_json::from_str(data)
			.map_err(|error| tg::error!(!error, "failed to deserialize the header value"))?;

		Err(error)
	});
	let stream = stream::select(data_messages, trailer_messages).attach(task);

	stream.boxed()
}

pub(super) struct StdioTaskArg<H> {
	pub handle: H,
	pub id: tg::process::Id,
	pub location: Option<tg::Location>,
	pub raw: bool,
	pub stderr: Option<tg::process::Stdio>,
	pub stdin: Option<tg::process::Stdio>,
	pub stdout: Option<tg::process::Stdio>,
	pub tokens: tg::authorization::Tokens,
	pub tty: bool,
}

pub(super) async fn stdio_task<H>(arg: StdioTaskArg<H>) -> tg::Result<()>
where
	H: tg::Handle,
{
	let StdioTaskArg {
		handle,
		id,
		location,
		raw,
		stderr,
		stdin,
		stdout,
		tokens,
		tty,
	} = arg;
	let mut stdin_task = stdin.map(|stdin| {
		let handle = handle.clone();
		let id = id.clone();
		let location = location.clone();
		let tokens = tokens.clone();
		Task::spawn(
			move |_| async move { stdin_task(&handle, id, location, stdin, raw, tokens).await },
		)
	});

	let sigwinch_task = if tty {
		let handle = handle.clone();
		let id = id.clone();
		let location = location.clone();
		let tokens = tokens.clone();
		let task =
			Task::spawn(|_| async move { sigwinch_task(&handle, id, location, tokens).await });
		Some(task)
	} else {
		None
	};

	let output = if stdout.is_some() || stderr.is_some() {
		let handle = handle.clone();
		let id = id.clone();
		let location = location.clone();
		stdout_stderr_task(&handle, id, location, stdout, stderr, tokens).await
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

async fn stdin_task<H>(
	handle: &H,
	id: tg::process::Id,
	location: Option<tg::Location>,
	stdin: tg::process::Stdio,
	raw: bool,
	tokens: tg::authorization::Tokens,
) -> tg::Result<()>
where
	H: tg::Handle,
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
	let arg = tg::process::stdio::write::Arg {
		location: location.map(Into::into),
		streams: vec![tg::process::stdio::Stream::Stdin],
		tokens,
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
	handle.write_process_stdio_all(&id, arg, input).await
}

async fn stdout_stderr_task<H>(
	handle: &H,
	id: tg::process::Id,
	location: Option<tg::Location>,
	stdout: Option<tg::process::Stdio>,
	stderr: Option<tg::process::Stdio>,
	tokens: tg::authorization::Tokens,
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
		location: location.map(Into::into),
		streams,
		tokens,
		..Default::default()
	};
	let Some(stream) = handle.try_read_process_stdio_all(&id, arg).await? else {
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

async fn sigwinch_task<H>(
	handle: &H,
	id: tg::process::Id,
	location: Option<tg::Location>,
	tokens: tg::authorization::Tokens,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())
		.map_err(|error| tg::error!(!error, "failed to create signal handler"))?;
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
			location: location.clone().map(Into::into),
			size,
			tokens: tokens.clone(),
		};
		handle
			.set_process_tty_size(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the tty"))?;
	}
	Ok(())
}
