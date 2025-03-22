use crate::Server;
use bytes::Bytes;
use futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream};
use itertools::Itertools as _;
use num::ToPrimitive;
use std::time::Duration;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio_stream::wrappers::IntervalStream;
use tokio_util::task::AbortOnDropHandle;

pub enum Reader {
	Blob(crate::blob::Reader),
	File(tokio::fs::File),
}

impl Server {
	pub async fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
	> {
		if let Some(stream) = self.try_get_process_log_local(id, arg.clone()).await? {
			Ok(Some(stream.left_stream()))
		} else if let Some(stream) = self.try_get_process_log_remote(id, arg.clone()).await? {
			Ok(Some(stream.right_stream()))
		} else {
			Ok(None)
		}
	}

	async fn try_get_process_log_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
	> {
		// Verify the process is local.
		if !self.get_process_exists_local(id).await? {
			return Ok(None);
		}

		// Create the channel.
		let (sender, receiver) = async_channel::unbounded();

		// Spawn the task.
		let server = self.clone();
		let id = id.clone();
		let task = tokio::spawn(async move {
			let result = server
				.try_get_process_log_local_task(&id, arg, sender.clone())
				.await;
			if let Err(error) = result {
				sender.try_send(Err(error)).ok();
			}
		});
		let abort_handle = AbortOnDropHandle::new(task);

		let stream = receiver.attach(abort_handle);

		Ok(Some(stream))
	}

	async fn try_get_process_log_local_task(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
		sender: async_channel::Sender<tg::Result<tg::process::log::get::Event>>,
	) -> tg::Result<()> {
		// Subscribe to log events.
		let subject = format!("processes.{id}.log");
		let log = self
			.messenger
			.subscribe(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();

		// Subscribe to status events.
		let subject = format!("processes.{id}.status");
		let status = self
			.messenger
			.subscribe(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();

		// Create the interval.
		let interval = IntervalStream::new(tokio::time::interval(Duration::from_secs(60)))
			.map(|_| ())
			.boxed();

		// Create the events stream.
		let mut events = stream::select_all([log, status, interval]).boxed();

		// Create the reader.
		let mut reader = Reader::new(self, id).await?;

		// Seek the reader.
		let seek = if let Some(position) = arg.position {
			position
		} else {
			std::io::SeekFrom::Start(0)
		};
		reader
			.seek(seek)
			.await
			.map_err(|source| tg::error!(!source, "failed to seek the stream"))?;

		// Create the state.
		let size = arg.size.unwrap_or(4096);
		let mut read = 0;

		loop {
			// Get the process's status.
			let status = self
				.try_get_current_process_status_local(id)
				.await?
				.ok_or_else(|| tg::error!(%process = id, "process does not exist"))?;

			// Send as many data events as possible.
			loop {
				// Get the position.
				let position = reader
					.stream_position()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the stream position"))?;

				// Determine the size.
				let size = match arg.length {
					None => size,
					Some(length) => {
						if length >= 0 {
							size.min(length.abs().to_u64().unwrap() - read)
						} else {
							size.min(length.abs().to_u64().unwrap() - read)
								.min(position)
						}
					},
				};

				// Seek if necessary.
				if arg.length.is_some_and(|length| length < 0) {
					let seek = std::io::SeekFrom::Current(-size.to_i64().unwrap());
					reader
						.seek(seek)
						.await
						.map_err(|source| tg::error!(!source, "failed to seek the reader"))?;
				}

				// Read the chunk.
				let position = reader
					.stream_position()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the stream position"))?;
				let mut data = vec![0u8; size.to_usize().unwrap()];
				let mut n = 0;
				while n < data.len() {
					let n_ = reader
						.read(&mut data[n..])
						.await
						.map_err(|source| tg::error!(!source, "failed to read from the reader"))?;
					n += n_;
					if n_ == 0 {
						break;
					}
				}
				data.truncate(n);
				let chunk = tg::process::log::get::Chunk {
					position,
					bytes: data.into(),
				};

				// Update the state.
				read += n.to_u64().unwrap();

				// Seek if necessary.
				if arg.length.is_some_and(|length| length < 0) {
					let seek = std::io::SeekFrom::Current(-n.to_i64().unwrap());
					reader
						.seek(seek)
						.await
						.map_err(|source| tg::error!(!source, "failed to seek the reader"))?;
				}

				// If the chunk is empty, then break.
				if chunk.bytes.is_empty() {
					break;
				}

				// Send the data.
				let result = sender.try_send(Ok(tg::process::log::get::Event::Chunk(chunk)));
				if result.is_err() {
					return Ok(());
				}
			}

			// If the process was finished or the length was reached, then send the end event and break.
			let end = if let Some(length) = arg.length {
				let position = reader
					.stream_position()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the stream position"))?;
				(read >= length.abs().to_u64().unwrap()) || (position == 0 && length < 0)
			} else {
				false
			};
			if end || status.is_finished() {
				let result = sender.try_send(Ok(tg::process::log::get::Event::End));
				if result.is_err() {
					return Ok(());
				}
				break;
			}

			// Wait for an event before returning to the top of the loop.
			events.next().await;
		}

		Ok(())
	}

	async fn try_get_process_log_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
	> {
		let futures = self
			.get_remote_clients()
			.await?
			.values()
			.map(|client| {
				{
					let client = client.clone();
					let id = id.clone();
					let arg = arg.clone();
					async move {
						client
							.get_process_log(&id, arg)
							.await
							.map(futures::StreamExt::boxed)
					}
				}
				.boxed()
			})
			.collect_vec();
		if futures.is_empty() {
			return Ok(None);
		}
		let Ok((stream, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		let stream = stream
			.map_ok(tg::process::log::get::Event::Chunk)
			.chain(stream::once(future::ok(tg::process::log::get::Event::End)));
		Ok(Some(stream))
	}

	pub async fn try_post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> tg::Result<tg::process::log::post::Output> {
		// If the remote arg is set, then forward the request.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let remote = self.get_remote_client(remote.clone()).await?;
			let arg = tg::process::log::post::Arg {
				remote: None,
				..arg
			};
			let output = remote.try_post_process_log(id, arg).await?;
			return Ok(output);
		}

		// Verify the process is local and started.
		if self.get_current_process_status_local(id).await? != tg::process::Status::Started {
			return Ok(tg::process::log::post::Output { added: false });
		}

		// Log.
		self.try_add_process_log_to_file(id, arg.bytes).await?;

		// Publish the message.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				server
					.messenger
					.publish(format!("processes.{id}.log"), Bytes::new())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		Ok(tg::process::log::post::Output { added: true })
	}

	async fn try_add_process_log_to_file(
		&self,
		id: &tg::process::Id,
		bytes: Bytes,
	) -> tg::Result<()> {
		let path = self.logs_path().join(format!("{id}"));
		let mut file = tokio::fs::File::options()
			.create(true)
			.append(true)
			.open(&path)
			.await
			.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to open the log file"),
			)?;
		file.write_all(&bytes).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to write to the log file"),
		)?;
		Ok(())
	}
}

impl Reader {
	pub async fn new(server: &Server, id: &tg::process::Id) -> tg::Result<Self> {
		// Attempt to create a blob reader.
		let output = server
			.try_get_process_local(id)
			.await?
			.ok_or_else(|| tg::error!("expected the process to exist"))?;
		if let Some(log) = output.data.log {
			let blob = tg::Blob::with_id(log);
			let reader = crate::blob::Reader::new(server, blob).await?;
			return Ok(Self::Blob(reader));
		}

		// Attempt to create a file reader.
		let path = server.logs_path().join(format!("{id}"));
		match tokio::fs::File::open(&path).await {
			Ok(file) => {
				return Ok(Self::File(file));
			},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
			Err(source) => {
				return Err(
					tg::error!(!source, %path = path.display(), "failed to open the log file"),
				);
			},
		}

		Err(tg::error!("failed to find the log"))
	}
}

impl AsyncRead for Reader {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		match self.get_mut() {
			Reader::Blob(reader) => std::pin::Pin::new(reader).poll_read(cx, buf),
			Reader::File(reader) => std::pin::Pin::new(reader).poll_read(cx, buf),
		}
	}
}

impl AsyncSeek for Reader {
	fn start_seek(
		self: std::pin::Pin<&mut Self>,
		position: std::io::SeekFrom,
	) -> std::io::Result<()> {
		match self.get_mut() {
			Reader::Blob(reader) => std::pin::Pin::new(reader).start_seek(position),
			Reader::File(reader) => std::pin::Pin::new(reader).start_seek(position),
		}
	}

	fn poll_complete(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		match self.get_mut() {
			Reader::Blob(reader) => std::pin::Pin::new(reader).poll_complete(cx),
			Reader::File(reader) => std::pin::Pin::new(reader).poll_complete(cx),
		}
	}
}

impl Server {
	pub(crate) async fn handle_get_process_log_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Get the stream.
		let Some(stream) = handle.try_get_process_log_stream(&id, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let stream = stream.take_until(stop);

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}

	pub(crate) async fn handle_post_process_log_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.try_post_process_log(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
