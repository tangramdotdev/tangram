use crate::Server;
use bytes::{Bytes, BytesMut};
use futures::{
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
	future::{self, BoxFuture},
	stream,
};
use indoc::formatdoc;
use itertools::Itertools as _;
use num::ToPrimitive;
use std::{io::Cursor, time::Duration};
use sync_wrapper::SyncWrapper;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio_stream::wrappers::IntervalStream;
use tokio_util::task::AbortOnDropHandle;

pub enum Reader {
	Blob(crate::blob::Reader),
	Database(DatabaseReader),
	File(tokio::fs::File),
}

pub struct DatabaseReader {
	cursor: Option<Cursor<Bytes>>,
	id: tg::process::Id,
	position: u64,
	read: Option<SyncWrapper<ReadFuture>>,
	seek: Option<SyncWrapper<SeekFuture>>,
	server: Server,
}

type ReadFuture = BoxFuture<'static, tg::Result<Option<Cursor<Bytes>>>>;

type SeekFuture = BoxFuture<'static, tg::Result<u64>>;

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
		if self.config.advanced.write_process_logs_to_database {
			self.try_add_process_log_to_database(id, arg.bytes).await?;
		} else {
			self.try_add_process_log_to_file(id, arg.bytes).await?;
		}

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

	async fn try_add_process_log_to_database(
		&self,
		id: &tg::process::Id,
		bytes: Bytes,
	) -> tg::Result<()> {
		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Add the log to the database.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into process_logs (process, bytes, position)
				values (
					{p}1,
					{p}2,
					(
						select coalesce(
							(
								select position + length(bytes)
								from process_logs
								where process = {p}1
								order by position desc
								limit 1
							),
							0
						)
					)
				);
			"
		);
		let params = db::params![id, bytes];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(())
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

		// Otherwise, create a database reader.
		let reader = DatabaseReader::new(server, id);
		Ok(Self::Database(reader))
	}
}

impl DatabaseReader {
	fn new(server: &Server, id: &tg::process::Id) -> Self {
		let cursor = None;
		let id = id.clone();
		let position = 0;
		let read = None;
		let seek = None;
		let server = server.clone();
		Self {
			cursor,
			id,
			position,
			read,
			seek,
			server,
		}
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
			Reader::Database(reader) => std::pin::Pin::new(reader).poll_read(cx, buf),
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
			Reader::Database(reader) => std::pin::Pin::new(reader).start_seek(position),
			Reader::File(reader) => std::pin::Pin::new(reader).start_seek(position),
		}
	}

	fn poll_complete(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		match self.get_mut() {
			Reader::Blob(reader) => std::pin::Pin::new(reader).poll_complete(cx),
			Reader::Database(reader) => std::pin::Pin::new(reader).poll_complete(cx),
			Reader::File(reader) => std::pin::Pin::new(reader).poll_complete(cx),
		}
	}
}

impl AsyncRead for DatabaseReader {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		let this = self.get_mut();

		// Create the read future if necessary.
		if this.cursor.is_none() && this.read.is_none() {
			let server = this.server.clone();
			let id = this.id.clone();
			let position = this.position;
			let length = (buf.capacity() - buf.filled().len()).to_u64().unwrap();
			let read = SyncWrapper::new(
				async move { poll_read_inner(server, &id, position, length).await }.boxed(),
			);
			this.read = Some(read);
		}

		// Poll the read future if necessary.
		if let Some(read) = this.read.as_mut() {
			match read.get_mut().as_mut().poll(cx) {
				std::task::Poll::Pending => return std::task::Poll::Pending,
				std::task::Poll::Ready(Err(error)) => {
					this.read.take();
					return std::task::Poll::Ready(Err(std::io::Error::other(error)));
				},
				std::task::Poll::Ready(Ok(None)) => {
					this.read.take();
					return std::task::Poll::Ready(Ok(()));
				},
				std::task::Poll::Ready(Ok(Some(cursor))) => {
					this.read.take();
					this.cursor.replace(cursor);
				},
			}
		}

		// Read.
		let cursor = this.cursor.as_mut().unwrap();
		let bytes = cursor.get_ref();
		let position = cursor.position().to_usize().unwrap();
		let n = std::cmp::min(buf.remaining(), bytes.len() - position);
		buf.put_slice(&bytes[position..position + n]);
		this.position += n as u64;
		let position = position + n;
		cursor.set_position(position as u64);
		if position == cursor.get_ref().len() {
			this.cursor.take();
		}
		std::task::Poll::Ready(Ok(()))
	}
}

async fn poll_read_inner(
	server: Server,
	id: &tg::process::Id,
	position: u64,
	length: u64,
) -> tg::Result<Option<Cursor<Bytes>>> {
	// Get a database connection.
	let connection = server
		.database
		.connection()
		.await
		.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

	// Get the rows.
	#[derive(serde::Deserialize)]
	struct Row {
		position: u64,
		bytes: Bytes,
	}
	let p = connection.p();
	let statement = formatdoc!(
		"
			select position, bytes
			from process_logs
			where process = {p}1 and (
				({p}2 < position and {p}2 + {p}3 > position) or
				({p}2 >= position and {p}2 < position + length(bytes))
			)
			order by position;
		"
	);
	let params = db::params![id, position, length];
	let rows = connection
		.query_all_into::<Row>(statement.into(), params)
		.await
		.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

	// Drop the database connection.
	drop(connection);

	let mut bytes = BytesMut::with_capacity(length.to_usize().unwrap());
	for row in rows {
		if row.position < position {
			let start = (position - row.position).to_usize().unwrap();
			bytes.extend_from_slice(&row.bytes[start..]);
		} else {
			bytes.extend_from_slice(&row.bytes);
		}
	}
	let cursor = Cursor::new(bytes.into());

	Ok(Some(cursor))
}

impl AsyncSeek for DatabaseReader {
	fn start_seek(
		mut self: std::pin::Pin<&mut Self>,
		seek: std::io::SeekFrom,
	) -> std::io::Result<()> {
		if self.seek.is_some() {
			return Err(std::io::Error::other("already seeking"));
		}
		let server = self.server.clone();
		let position = self.position;
		let id = self.id.clone();
		let seek = SyncWrapper::new(
			async move { poll_seek_inner(server, &id, position, seek).await }.boxed(),
		);
		self.seek = Some(seek);
		Ok(())
	}

	fn poll_complete(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		let Some(seek) = self.seek.as_mut() else {
			return std::task::Poll::Ready(Ok(self.position));
		};
		let position = match seek.get_mut().as_mut().poll(cx) {
			std::task::Poll::Ready(Ok(position)) => {
				self.seek.take();
				position
			},
			std::task::Poll::Ready(Err(error)) => {
				self.seek.take();
				return std::task::Poll::Ready(Err(std::io::Error::other(error)));
			},
			std::task::Poll::Pending => {
				return std::task::Poll::Pending;
			},
		};
		self.position = position;
		self.cursor = None;
		std::task::Poll::Ready(Ok(position))
	}
}

async fn poll_seek_inner(
	server: Server,
	id: &tg::process::Id,
	position: u64,
	seek: std::io::SeekFrom,
) -> tg::Result<u64> {
	// Get a database connection.
	let connection = server
		.database
		.connection()
		.await
		.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

	// Get the end.
	let p = connection.p();
	let statement = formatdoc!(
		"
			select coalesce(
				(
					select position + length(bytes)
					from process_logs
					where process = {p}1 and position = (
						select max(position)
						from process_logs
						where process = {p}1
					)
				),
				0
			);
		"
	);
	let params = db::params![id];
	let end = connection
		.query_one_value_into::<u64>(statement.into(), params)
		.await
		.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

	// Drop the database connection.
	drop(connection);

	let position = match seek {
		std::io::SeekFrom::Start(seek) => seek.to_i64().unwrap(),
		std::io::SeekFrom::End(seek) => end.to_i64().unwrap() + seek,
		std::io::SeekFrom::Current(seek) => position.to_i64().unwrap() + seek,
	};
	let position = position.to_u64().ok_or(tg::error!(
		%position,
		"attempted to seek to a negative or overflowing position",
	))?;
	if position > end {
		return Err(tg::error!(%position, %end, "attempted to seek to a position beyond the end"));
	}

	Ok(position)
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
		handle.try_post_process_log(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
