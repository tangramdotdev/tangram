use crate::{Http, Server};
use bytes::{Bytes, BytesMut};
use futures::{
	future::{self, BoxFuture},
	stream::{self, BoxStream},
	stream_select, FutureExt, StreamExt, TryStreamExt,
};
use http_body_util::{BodyExt, StreamBody};
use indoc::formatdoc;
use num::ToPrimitive;
use std::{io::Cursor, sync::Arc};
use tangram_client as tg;
use tangram_database as db;
use tangram_error::{error, Error, Result};
use tangram_http::{empty, not_found, Incoming, Outgoing};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use tokio_stream::wrappers::IntervalStream;

pub enum Reader {
	Blob(tg::blob::Reader),
	Database(DatabaseReader),
}

pub struct DatabaseReader {
	cursor: Option<Cursor<Bytes>>,
	id: tg::build::Id,
	position: u64,
	read: Option<BoxFuture<'static, Result<Option<Cursor<Bytes>>>>>,
	seek: Option<BoxFuture<'static, Result<u64>>>,
	server: Server,
}

unsafe impl Sync for DatabaseReader {}

impl Server {
	pub async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::log::Chunk>>>> {
		if let Some(log) = self
			.try_get_build_log_local(id, arg.clone(), stop.clone())
			.await?
		{
			Ok(Some(log))
		} else if let Some(log) = self
			.try_get_build_log_remote(id, arg.clone(), stop.clone())
			.await?
		{
			Ok(Some(log))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_log_local(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::log::Chunk>>>> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Create the event stream.
		let log = self.inner.messenger.subscribe_to_build_log(id).await?;
		let status = self.inner.messenger.subscribe_to_build_status(id).await?;
		let interval =
			IntervalStream::new(tokio::time::interval(std::time::Duration::from_secs(60)))
				.map(|_| ());
		let timeout = arg.timeout.map_or_else(
			|| future::pending().left_future(),
			|timeout| tokio::time::sleep(timeout).right_future(),
		);
		let stop = stop.map_or_else(
			|| future::pending().left_future(),
			|mut stop| async move { stop.wait_for(|stop| *stop).map(|_| ()).await }.right_future(),
		);
		let events = stream::once(future::ready(()))
			.chain(
				stream_select!(log, status, interval)
					.take_until(timeout)
					.take_until(stop),
			)
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
			.map_err(|source| error!(!source, "failed to seek the stream"))?;

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
				if end {
					return Ok(None);
				}

				if let Some(length) = length {
					let state = state.lock().await;
					if (state.read >= length.abs().to_u64().unwrap())
						|| (state.reader.position() == 0 && length < 0)
					{
						return Ok(None);
					}
				}

				let Some(()) = events.next().await else {
					return Ok(None);
				};

				let status = server
					.try_get_build_status_local(
						&id,
						tg::build::status::GetArg {
							timeout: Some(std::time::Duration::ZERO),
						},
						None,
					)
					.await?
					.ok_or_else(|| error!("expected the build to exist"))?
					.try_next()
					.await?
					.ok_or_else(|| error!("expected the status to exist"))?;
				if status == tg::build::Status::Finished {
					end = true;
				}

				// Create the stream.
				let stream =
					stream::try_unfold(
						(server.clone(), id.clone(), state.clone(), false),
						move |(server, id, state, end)| async move {
							if end {
								return Ok(None);
							}

							// Lock the state.
							let mut state_ = state.lock().await;

							// Determine the size.
							let size = match length {
								None => size,
								Some(length) => {
									if length >= 0 {
										size.min(length.abs().to_u64().unwrap() - state_.read)
									} else {
										size.min(length.abs().to_u64().unwrap() - state_.read)
											.min(state_.reader.position())
									}
								},
							};

							// Seek if necessary.
							if length.is_some_and(|length| length < 0) {
								let seek = std::io::SeekFrom::Current(-size.to_i64().unwrap());
								state_.reader.seek(seek).await.map_err(|source| {
									error!(!source, "failed to seek the reader")
								})?;
							}

							// Read the chunk.
							let position = state_.reader.position();
							let mut data = vec![0u8; size.to_usize().unwrap()];
							let mut read = 0;
							while read < data.len() {
								let n = state_.reader.read(&mut data[read..]).await.map_err(
									|source| error!(!source, "failed to read from the reader"),
								)?;
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
									error!(!source, "failed to seek the reader")
								})?;
							}

							// If the chunk is empty, then only return it if the build is finished and the position is at the end.
							if chunk.bytes.is_empty() {
								if state_.reader.end().await? {
									drop(state_);
									return Ok::<_, Error>(Some((
										chunk,
										(server, id, state, true),
									)));
								}

								drop(state_);
								return Ok(None);
							}

							drop(state_);
							Ok::<_, Error>(Some((chunk, (server, id, state, end))))
						},
					);

				Ok::<_, Error>(Some((stream, (events, server, id, state, end))))
			},
		)
		.try_flatten()
		.boxed();

		Ok(Some(stream))
	}

	async fn try_get_build_log_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::log::Chunk>>>> {
		let Some(remote) = self.inner.remotes.first() else {
			return Ok(None);
		};
		let Some(log) = remote.try_get_build_log(id, arg, stop).await? else {
			return Ok(None);
		};
		Ok(Some(log))
	}

	pub async fn add_build_log(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<()> {
		if self
			.try_add_build_log_local(user, id, bytes.clone())
			.await?
		{
			return Ok(());
		}
		if self
			.try_add_build_log_remote(user, id, bytes.clone())
			.await?
		{
			return Ok(());
		}
		Err(error!("failed to get the build"))
	}

	async fn try_add_build_log_local(
		&self,
		_user: Option<&tg::User>,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<bool> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(false);
		}

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

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
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Publish the message.
		self.inner.messenger.publish_to_build_log(id).await?;

		Ok(true)
	}

	async fn try_add_build_log_remote(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<bool> {
		let Some(remote) = self.inner.remotes.first() else {
			return Ok(false);
		};
		remote.add_build_log(user, id, bytes).await?;
		Ok(true)
	}
}

impl Reader {
	pub async fn new(server: &Server, id: &tg::build::Id) -> Result<Self> {
		let output = server
			.try_get_build_local(id)
			.await?
			.ok_or_else(|| error!("expected the build to exist"))?;
		if let Some(log) = output.log {
			let blob = tg::Blob::with_id(log);
			let reader = blob.reader(server).await?;
			Ok(Self::Blob(reader))
		} else {
			let reader = DatabaseReader::new(server, id);
			Ok(Self::Database(reader))
		}
	}

	pub fn position(&self) -> u64 {
		match self {
			Reader::Blob(reader) => reader.position(),
			Reader::Database(reader) => reader.position(),
		}
	}

	pub async fn end(&self) -> Result<bool> {
		match self {
			Reader::Blob(reader) => Ok(reader.end()),
			Reader::Database(reader) => reader.end().await,
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

	fn position(&self) -> u64 {
		self.position
	}

	pub async fn end(&self) -> Result<bool> {
		// Get a database connection.
		let connection = self
			.server
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

		// Get the end.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select (
					select status = 'finished'
					from builds
					where id = {p}1
				) and (
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
					)
				);
			"
		);
		let params = db::params![self.id, self.position];
		let end = connection
			.query_one_scalar_into(statement, params)
			.await
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

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
		}
	}

	fn poll_complete(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		match self.get_mut() {
			Reader::Blob(reader) => std::pin::Pin::new(reader).poll_complete(cx),
			Reader::Database(reader) => std::pin::Pin::new(reader).poll_complete(cx),
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
			let read = async move { poll_read_inner(server, id, position, length).await }.boxed();
			this.read = Some(read);
		}

		// Poll the read future if necessary.
		if let Some(read) = this.read.as_mut() {
			match read.as_mut().poll(cx) {
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
) -> Result<Option<Cursor<Bytes>>> {
	// Get a database connection.
	let connection = server
		.inner
		.database
		.connection()
		.await
		.map_err(|source| error!(!source, "failed to get a database connection"))?;

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
		.map_err(|source| error!(!source, "failed to perform query"))?;

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
		let seek = async move { poll_seek_inner(server, id, position, seek).await }.boxed();
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
		let position = match seek.as_mut().poll(cx) {
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
) -> Result<u64> {
	// Get a database connection.
	let connection = server
		.inner
		.database
		.connection()
		.await
		.map_err(|source| error!(!source, "failed to get a database connection"))?;

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
		.query_one_scalar_into::<u64>(statement, params)
		.await
		.map_err(|source| error!(!source, "failed to execute the statement"))?;

	// Drop the database connection.
	drop(connection);

	let position = match seek {
		std::io::SeekFrom::Start(seek) => seek.to_i64().unwrap(),
		std::io::SeekFrom::End(seek) => end.to_i64().unwrap() + seek,
		std::io::SeekFrom::Current(seek) => position.to_i64().unwrap() + seek,
	};
	let position = position.to_u64().ok_or(error!(
		%position,
		"attempted to seek to a negative or overflowing position",
	))?;
	if position > end {
		return Err(error!(%position, %end, "attempted to seek to a position beyond the end"));
	}

	Ok(position)
}

impl Http {
	pub async fn handle_get_build_log_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "log"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let id = id
			.parse()
			.map_err(|source| error!(!source, "failed to parse the ID"))?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.map_err(|source| error!(!source, "failed to deserialize the search params"))?
			.unwrap_or_default();

		// Get the accept header.
		let accept = request
			.headers()
			.get(http::header::ACCEPT)
			.map(|accept| {
				let accept = accept
					.to_str()
					.map_err(|source| error!(!source, "invalid content type"))?;
				let accept = accept
					.parse::<mime::Mime>()
					.map_err(|source| error!(!source, "invalid content type"))?;
				Ok::<_, Error>(accept)
			})
			.transpose()?;

		let stop = request.extensions().get().cloned().unwrap();
		let Some(stream) = self
			.inner
			.tg
			.try_get_build_log(&id, arg, Some(stop))
			.await?
		else {
			return Ok(not_found());
		};

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let body = stream
					.map_ok(|chunk| {
						let data = serde_json::to_string(&chunk).unwrap();
						let event = tangram_sse::Event::with_data(data);
						hyper::body::Frame::data(event.to_string().into())
					})
					.map_err(Into::into);
				(content_type, body)
			},
			_ => {
				return Err(error!(?accept, "invalid accept header"));
			},
		};
		let body = Outgoing::new(StreamBody::new(body));

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
	}

	pub async fn handle_add_build_log_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "log"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let build_id = id
			.parse()
			.map_err(|source| error!(!source, "failed to parse the ID"))?;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| error!(!source, "failed to read the body"))?
			.to_bytes();

		self.inner
			.tg
			.add_build_log(user.as_ref(), &build_id, bytes)
			.await?;

		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}
}
