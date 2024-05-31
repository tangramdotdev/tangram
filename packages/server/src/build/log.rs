use crate::Server;
use bytes::{Bytes, BytesMut};
use futures::{
	future::{self, BoxFuture},
	stream, stream_select, FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
};
use indoc::formatdoc;
use itertools::Itertools as _;
use num::ToPrimitive as _;
use std::{io::Cursor, pin::pin, sync::Arc};
use sync_wrapper::SyncWrapper;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_futures::task::Stop;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tangram_messenger::Messenger as _;
use tg::Handle as _;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio_stream::wrappers::IntervalStream;

pub enum Reader {
	Blob(tg::blob::Reader<Server>),
	Database(DatabaseReader),
	File(tokio::fs::File),
}

pub struct DatabaseReader {
	cursor: Option<Cursor<Bytes>>,
	id: tg::build::Id,
	position: u64,
	read: Option<SyncWrapper<ReadFuture>>,
	seek: Option<SyncWrapper<SeekFuture>>,
	server: Server,
}

type ReadFuture = BoxFuture<'static, tg::Result<Option<Cursor<Bytes>>>>;

type SeekFuture = BoxFuture<'static, tg::Result<u64>>;

impl Server {
	pub async fn try_get_build_log_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::log::Event>> + Send + 'static>>
	{
		let log = if let Some(log) = self.try_get_build_log_local(id, arg.clone()).await? {
			log.left_stream()
		} else if let Some(log) = self.try_get_build_log_remote(id, arg.clone()).await? {
			log.right_stream()
		} else {
			return Ok(None);
		};
		let end = stream::once(async move {
			Ok::<_, tg::Error>(tg::build::log::Event::End)
		});
		let log = log
			.map(|data| data.map(tg::build::log::Event::Data))
			.chain(end);
		Ok(Some(log))
	}

	async fn try_get_build_log_local(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>>
	{
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Create the event stream.
		let log = self
			.messenger
			.subscribe(format!("builds.{id}.log"), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();
		let status = self
			.messenger
			.subscribe(format!("builds.{id}.status"), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();
		let interval =
			IntervalStream::new(tokio::time::interval(std::time::Duration::from_secs(60)))
				.map(|_| ())
				.skip(1);
		let timeout = arg.timeout.map_or_else(
			|| future::pending().left_future(),
			|timeout| tokio::time::sleep(timeout).right_future(),
		);
		let events = stream::once(future::ready(()))
			.chain(stream_select!(log, status, interval).take_until(timeout))
			.boxed();

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

		// Get the length.
		let length = arg.length;

		// Get the size.
		let size = arg.size.unwrap_or(4096);

		// Create the stream.
		struct State {
			read: u64,
			reader: Reader,
		}
		let state = Arc::new(tokio::sync::Mutex::new(State { read: 0, reader }));
		let server = self.clone();
		let id = id.clone();
		let stream = stream::try_unfold(
			(events, server, id, state, false),
			move |(mut events, server, id, state, mut end)| async move {
				// If the stream should end, return None.
				if end {
					return Ok(None);
				}

				// Check if the length has been reached.
				if let Some(length) = length {
					let mut state = state.lock().await;
					let position = state.reader.stream_position().await.map_err(|source| {
						tg::error!(!source, "failed to get the stream position")
					})?;
					if (state.read >= length.abs().to_u64().unwrap())
						|| (position == 0 && length < 0)
					{
						return Ok(None);
					}
				}

				// Wait for the next event.
				let Some(()) = events.next().await else {
					return Ok(None);
				};

				// Get the build status.
				let arg = tg::build::status::Arg {
					timeout: Some(std::time::Duration::ZERO),
				};
				let status = server
					.try_get_build_status_local(&id, arg)
					.await?
					.ok_or_else(|| tg::error!("expected the build to exist"))?;
				let status = pin!(status)
					.try_next()
					.await?
					.ok_or_else(|| tg::error!("expected the status to exist"))?;
				if status == tg::build::Status::Finished {
					end = true;
				}

				// Create the stream.
				let stream = stream::try_unfold(
					(server.clone(), id.clone(), state.clone(), false),
					move |(server, id, state, end)| async move {
						if end {
							return Ok(None);
						}

						// Lock the state.
						let mut state_ = state.lock().await;

						// Determine the size.
						let position = state_.reader.stream_position().await.map_err(|source| {
							tg::error!(!source, "failed to get the stream position")
						})?;
						let size = match length {
							None => size,
							Some(length) => {
								if length >= 0 {
									size.min(length.abs().to_u64().unwrap() - state_.read)
								} else {
									size.min(length.abs().to_u64().unwrap() - state_.read)
										.min(position)
								}
							},
						};

						// Seek if necessary.
						if length.is_some_and(|length| length < 0) {
							let seek = std::io::SeekFrom::Current(-size.to_i64().unwrap());
							state_.reader.seek(seek).await.map_err(|source| {
								tg::error!(!source, "failed to seek the reader")
							})?;
						}

						// Read the chunk.
						let position = state_.reader.stream_position().await.map_err(|source| {
							tg::error!(!source, "failed to get the stream position")
						})?;
						let mut data = vec![0u8; size.to_usize().unwrap()];
						let mut read = 0;
						while read < data.len() {
							let n =
								state_
									.reader
									.read(&mut data[read..])
									.await
									.map_err(|source| {
										tg::error!(!source, "failed to read from the reader")
									})?;
							read += n;
							if n == 0 {
								break;
							}
						}
						data.truncate(read);
						let chunk = tg::build::log::Chunk {
							position,
							bytes: data.into(),
						};

						// Update the state.
						state_.read += read.to_u64().unwrap();

						// Seek if necessary.
						if length.is_some_and(|length| length < 0) {
							let seek = std::io::SeekFrom::Current(-read.to_i64().unwrap());
							state_.reader.seek(seek).await.map_err(|source| {
								tg::error!(!source, "failed to seek the reader")
							})?;
						}

						// If the chunk is empty, then only return it if the build is finished and the reader is at the end.
						if chunk.bytes.is_empty() {
							if status == tg::build::Status::Finished && state_.reader.end().await? {
								drop(state_);
								return Ok::<_, tg::Error>(Some((
									chunk,
									(server, id, state, true),
								)));
							}

							drop(state_);
							return Ok(None);
						}

						drop(state_);

						Ok::<_, tg::Error>(Some((chunk, (server, id, state, end))))
					},
				);

				Ok::<_, tg::Error>(Some((stream, (events, server, id, state, end))))
			},
		)
		.try_flatten()
		.boxed();

		Ok(Some(stream))
	}

	async fn try_get_build_log_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>>
	{
		let futures = self
			.remotes
			.iter()
			.map(|remote| {
				{
					let remote = remote.clone();
					let id = id.clone();
					let arg = arg.clone();
					async move {
						remote
							.get_build_log(&id, arg)
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
		Ok(Some(stream))
	}

	pub async fn add_build_log(&self, id: &tg::build::Id, bytes: Bytes) -> tg::Result<()> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Err(tg::error!("failed to find the build"));
		}

		if self.options.advanced.write_build_logs_to_database {
			self.try_add_build_log_database(id, bytes).await?;
		} else {
			self.try_add_build_log_file(id, bytes).await?;
		}

		Ok(())
	}

	async fn try_add_build_log_file(&self, id: &tg::build::Id, bytes: Bytes) -> tg::Result<()> {
		// Write to the log file.
		let path = self.logs_path().join(id.to_string());
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

		// Publish the message.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				server
					.messenger
					.publish(format!("builds.{id}.log"), Bytes::new())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		Ok(())
	}

	async fn try_add_build_log_database(&self, id: &tg::build::Id, bytes: Bytes) -> tg::Result<()> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Add the log to the database.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into build_logs (build, position, bytes)
				values (
					{p}1,
					(
						select coalesce(
							(
								select position + length(bytes)
								from build_logs
								where build = {p}1
								order by position desc
								limit 1
							),
							0
						)
					),
					{p}2
				);
			"
		);
		let params = db::params![id, bytes];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Publish the message.
		self.messenger
			.publish(format!("builds.{id}.log"), Bytes::new())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish"))?;

		Ok(())
	}
}

impl Reader {
	pub async fn new(server: &Server, id: &tg::build::Id) -> tg::Result<Self> {
		// Attempt to create a blob reader.
		let output = server
			.try_get_build_local(id)
			.await?
			.ok_or_else(|| tg::error!("expected the build to exist"))?;
		if let Some(log) = output.log {
			let blob = tg::Blob::with_id(log);
			let reader = blob.reader(server).await?;
			return Ok(Self::Blob(reader));
		}

		// Attempt to create a file reader.
		let path = server.logs_path().join(id.to_string());
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
		};

		// Otherwise, create a database reader.
		let reader = DatabaseReader::new(server, id);
		Ok(Self::Database(reader))
	}

	pub async fn end(&mut self) -> tg::Result<bool> {
		match self {
			Reader::Blob(reader) => Ok(reader.end()),
			Reader::Database(reader) => reader.end().await,
			Reader::File(file) => {
				let position = file
					.stream_position()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the position"))?;
				let size = file
					.metadata()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the metadata"))?
					.len();
				Ok(position == size)
			},
		}
	}
}

impl DatabaseReader {
	fn new(server: &Server, id: &tg::build::Id) -> Self {
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

	pub async fn end(&self) -> tg::Result<bool> {
		// Get a database connection.
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Determine if the position is at the end.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select coalesce(
					(
						select {p}2 >= position + length(bytes)
						from build_logs
						where build = {p}1 and position = (
							select max(position)
							from build_logs
							where build = {p}1
						)
					),
					true
				);
			"
		);
		let params = db::params![self.id, self.position];
		let end = connection
			.query_one_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(end)
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
				async move { poll_read_inner(server, id, position, length).await }.boxed(),
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
			};
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
	id: tg::build::Id,
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
			from build_logs
			where build = {p}1 and (
				({p}2 < position and {p}2 + {p}3 > position) or
				({p}2 >= position and {p}2 < position + length(bytes))
			)
			order by position;
		"
	);
	let params = db::params![id, position, length];
	let rows = connection
		.query_all_into::<Row>(statement, params)
		.await
		.map_err(|source| tg::error!(!source, "failed to perform query"))?;

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
			async move { poll_seek_inner(server, id, position, seek).await }.boxed(),
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
	id: tg::build::Id,
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
					from build_logs
					where build = {p}1 and position = (
						select max(position)
						from build_logs
						where build = {p}1
					)
				),
				0
			);
		"
	);
	let params = db::params![id];
	let end = connection
		.query_one_value_into::<u64>(statement, params)
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
	pub(crate) async fn handle_get_build_log_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Get the stream.
		let Some(stream) = handle.try_get_build_log_stream(&id, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stream = stream.take_until(async move { stop.stopped().await });

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let sse = stream.map(|result| match result {
					Ok(tg::build::log::Event::Data(data)) => {
						let data = serde_json::to_string(&data).unwrap();
						Ok::<_, tg::Error>(tangram_http::sse::Event::with_data(data))
					},
					Ok(tg::build::log::Event::End) => {
						let event = "end".to_owned();
						Ok::<_, tg::Error>(tangram_http::sse::Event {
							event: Some(event),
							..Default::default()
						})
					},
					Err(error) => {
						let data = serde_json::to_string(&error).unwrap();
						let event = "error".to_owned();
						Ok::<_, tg::Error>(tangram_http::sse::Event {
							data,
							event: Some(event),
							..Default::default()
						})
					},
				});
				let body = Outgoing::sse(sse);
				(content_type, body)
			},
			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
	}

	pub(crate) async fn handle_add_build_log_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let bytes = request.bytes().await?;
		handle.add_build_log(&id, bytes).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
