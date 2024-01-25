use crate::{params, Server};
use bytes::Bytes;
use futures::{
	future::{self, BoxFuture},
	stream::{self, BoxStream},
	FutureExt, StreamExt, TryStreamExt,
};
use http_body_util::{BodyExt, StreamBody};
use num::ToPrimitive;
use std::{io::Cursor, sync::Arc};
use tangram_client as tg;
use tangram_error::{error, Error, Result, WrapErr};
use tangram_util::http::{empty, not_found, Incoming, Outgoing};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use tokio_stream::wrappers::WatchStream;
use tokio_util::either::Either;

pub enum Reader {
	Blob(tg::blob::Reader),
	Database(DatabaseReader),
}

pub struct DatabaseReader {
	server: Server,
	id: tg::build::Id,
	offset: u64,
	row_offset: u64,
	read_state: State,
	seek_state: Option<SeekState>,
}

enum State {
	Empty,
	Reading(BoxFuture<'static, Result<Option<ReadSuccess>>>),
	Full(Cursor<Bytes>),
}

enum SeekState {
	Empty(std::io::SeekFrom),
	Seeking(BoxFuture<'static, Result<SeekSuccess>>),
}

struct ReadSuccess {
	data: Bytes,
	row_offset: u64,
}

struct SeekSuccess {
	offset: u64,
	row_offset: u64,
}

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

	#[allow(clippy::too_many_lines)]
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

		let server = self.clone();

		// Create the reader.
		let mut reader = Reader::new(self, id).await?;

		let context = self.inner.build_context.read().unwrap().get(id).cloned();

		let log = context.as_ref().map_or_else(
			|| stream::empty().boxed(),
			|context| WatchStream::from_changes(context.log.as_ref().unwrap().subscribe()).boxed(),
		);

		let status = context.as_ref().map_or_else(
			|| stream::empty().boxed(),
			|context| {
				WatchStream::from_changes(context.status.as_ref().unwrap().subscribe()).boxed()
			},
		);

		let timeout = arg
			.timeout
			.map(|timeout| tokio::time::sleep(timeout).boxed())
			.map_or_else(|| Either::Left(future::pending()), Either::Right);

		let stop = stop
			.map(|mut stop| async move { stop.wait_for(|stop| *stop).map(|_| ()).await })
			.map_or_else(|| Either::Left(future::pending()), Either::Right);

		let events = stream::once(future::ready(()))
			.chain(stream::select(log, status))
			.take_until(timeout)
			.take_until(stop)
			.boxed();

		// Seek in the reader using the argument offset/limit if they exist.
		let offset = if let Some(offset) = arg.offset {
			reader
				.seek(std::io::SeekFrom::Start(offset))
				.await
				.wrap_err("Failed to seek stream.")?
		} else {
			reader
				.seek(std::io::SeekFrom::End(0))
				.await
				.wrap_err("Failed to seek stream.")?
		};

		let read = 0;
		let limit = arg.limit;
		let chunk_size = arg.size.unwrap_or(4096);
		struct InnerStreamState {
			reader: Reader,
			read: usize,
			offset: u64,
		}
		let state = Arc::new(tokio::sync::Mutex::new(InnerStreamState {
			reader,
			read,
			offset,
		}));
		let id = id.clone();
		let stream = stream::try_unfold(
			(events, server, id, state),
			move |(mut events, server, id, state)| async move {
				// If the incoming stream of events is empty, we're done.
				let Some(()) = events.next().await else {
					return Ok(None);
				};

				// If we've already read up to the limit we don't need to create another inner stream.
				if let Some(limit) = limit {
					let state = state.lock().await;
					if state.read >= limit.abs().to_usize().unwrap() {
						return Ok(None);
					}
				}

				// Create the inner stream.
				let inner_stream = stream::try_unfold(
					(server.clone(), id.clone(), state.clone(), false),
					move |(server, id, state, end)| async move {
						if end {
							return Ok(None);
						}

						let mut state_ = state.lock().await;
						let offset = state_.offset;

						// Truncate the chunk size if necessary
						let chunk_size = if let Some(limit) = limit {
							let limit = limit.abs().to_usize().unwrap();
							chunk_size.to_usize().unwrap().min(limit - state_.read)
						} else {
							chunk_size.to_usize().unwrap()
						};

						// Read the next log chunk.
						let mut data = vec![0u8; chunk_size];
						let mut len = 0;
						while len < data.len() {
							len += state_
								.reader
								.read(&mut data[len..])
								.await
								.wrap_err("Failed to read from the reader.")?;
							if len == 0 {
								break;
							}
						}

						if len == 0 && !server.try_get_build_log_local_end(&id, offset).await? {
							return Ok(None);
						}

						data.truncate(len);
						state_.read += len;

						// If the stream is reading chunks backwards, seek the reader (len + chunk_size) behind the current position.
						if matches!(limit, Some(limit) if limit < 0) {
							let position = -(len.to_i64().unwrap() + chunk_size.to_i64().unwrap());
							let offset = state_
								.reader
								.seek(std::io::SeekFrom::Current(position))
								.await
								.wrap_err("Failed to seek backwards.")?;
							state_.offset = offset;
						} else {
							state_.offset += len.to_u64().unwrap();
						}

						// Create the chunk.
						let data = Bytes::from(data);
						let chunk = tg::build::log::Chunk { offset, data };

						// Check if this is the last chunk in the stream.
						let end = match limit {
							None => len == 0,
							Some(limit) => {
								state_.read >= limit.abs().to_usize().unwrap()
									|| (len == 0 && limit >= 0)
							},
						};
						drop(state_);
						Ok::<_, Error>(Some((chunk, (server, id, state, end))))
					},
				);

				Ok::<_, Error>(Some((inner_stream, (events, server, id, state))))
			},
		)
		.try_flatten()
		.boxed();

		Ok(Some(stream))
	}

	async fn try_get_build_log_local_end(&self, id: &tg::build::Id, offset: u64) -> Result<bool> {
		let db = self.inner.database.get().await?;
		let statement = "
		select
			(case
				when ?1 >= max_offset and build.state->>'status' = 'finished'
				then true
				else false
			end)
		from (
			select max(offset) as max_offset
			from build_logs
			where build = ?2
		)
		join
			builds build on build.id = ?2
		";

		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare statement.")?;
		let params = params![offset, id.to_string()];
		let mut query = statement
			.query(params)
			.wrap_err("Failed to perform query.")?;
		let row = query
			.next()
			.wrap_err("Failed to get row.")?
			.wrap_err("Expected a row.")?;
		let end = row.get(0).wrap_err("Expected a result.")?;
		Ok(end)
	}

	async fn try_get_build_log_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::log::Chunk>>>> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};
		let Some(log) = remote.try_get_build_log(id, arg).await? else {
			return Ok(None);
		};
		let stop = stop.map_or_else(
			|| future::pending().boxed(),
			|mut stop| async move { stop.wait_for(|s| *s).map(|_| ()).await }.boxed(),
		);
		let log = log.take_until(stop).boxed();
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
		Err(error!("Failed to get the build."))
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

		// Add the log to the database.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				insert into build_logs (build, offset, data)
				values (
					?1,
					(
						select coalesce(
							(
								select offset + length(data)
								from build_logs
								where build = ?1
								order by offset desc
								limit 1
							),
							0
						)
					),
					?2
				);
			";
			let params = params![id.to_string(), bytes.to_vec()];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Notify subscribers that the log has been added to.
		if let Some(log) = self
			.inner
			.build_context
			.read()
			.unwrap()
			.get(id)
			.unwrap()
			.log
			.as_ref()
		{
			log.send_replace(());
		}

		Ok(true)
	}

	async fn try_add_build_log_remote(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<bool> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};
		remote.add_build_log(user, id, bytes).await?;
		Ok(true)
	}
}

impl Reader {
	async fn new(server: &Server, id: &tg::build::Id) -> Result<Self> {
		let build = server
			.try_get_build_local(id)
			.await?
			.wrap_err("Expected a local build.")?;
		if let Some(log) = build.state.log {
			let blob = tg::Blob::with_id(log);
			let reader = blob.reader(server).await?;
			Ok(Self::Blob(reader))
		} else {
			let reader = DatabaseReader::new(server, id);
			Ok(Self::Database(reader))
		}
	}
}

impl DatabaseReader {
	fn new(server: &Server, id: &tg::build::Id) -> Self {
		let server = server.clone();
		let id = id.clone();
		let offset = 0;
		let row_offset = 0;
		let read_state = State::Empty;
		let seek_state = None;
		Self {
			server,
			id,
			offset,
			row_offset,
			read_state,
			seek_state,
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
		loop {
			match &mut this.read_state {
				// There is no state. Create a new future.
				State::Empty => {
					let limit = (buf.capacity() - buf.filled().len()).to_u64().unwrap();
					let future =
						poll_read_impl(&this.server, &this.id, this.offset, limit, this.row_offset);
					this.read_state = State::Reading(future);
				},
				State::Reading(future) => match future.as_mut().poll(cx) {
					std::task::Poll::Pending => return std::task::Poll::Pending,
					std::task::Poll::Ready(Err(error)) => {
						return std::task::Poll::Ready(Err(std::io::Error::new(
							std::io::ErrorKind::Other,
							error,
						)))
					},
					std::task::Poll::Ready(Ok(Some(success))) => {
						let ReadSuccess {
							data, row_offset, ..
						} = success;
						this.row_offset = row_offset;

						let data = Cursor::new(data);
						this.read_state = State::Full(data);
					},
					std::task::Poll::Ready(Ok(None)) => {
						this.read_state = State::Empty;
						return std::task::Poll::Ready(Ok(()));
					},
				},
				// We've polled a buffer already.
				State::Full(reader) => {
					let data = reader.get_ref();
					let position = reader.position().to_usize().unwrap();
					let n = std::cmp::min(buf.remaining(), data.len() - position);
					buf.put_slice(&data[position..position + n]);
					this.offset += n as u64;
					let position = position + n;
					if position == reader.get_ref().len() {
						this.read_state = State::Empty;
					} else {
						reader.set_position(position as u64);
					}
					return std::task::Poll::Ready(Ok(()));
				},
			}
		}
	}
}

impl AsyncSeek for DatabaseReader {
	fn start_seek(
		mut self: std::pin::Pin<&mut Self>,
		position: std::io::SeekFrom,
	) -> std::io::Result<()> {
		if self.seek_state.is_none() {
			self.seek_state.replace(SeekState::Empty(position));
			Ok(())
		} else {
			Err(std::io::Error::other("New seek is currently pending."))
		}
	}

	fn poll_complete(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		loop {
			match &mut self.seek_state {
				Some(SeekState::Empty(position)) => {
					let position = *position;
					let future = poll_seek_impl(&self.server, &self.id, self.offset, position);
					self.seek_state.replace(SeekState::Seeking(future));
				},
				Some(SeekState::Seeking(future)) => match future.as_mut().poll(cx) {
					std::task::Poll::Ready(Ok(success)) => {
						self.offset = success.offset;
						self.row_offset = success.row_offset;
						self.read_state = State::Empty;
						self.seek_state.take();
						return std::task::Poll::Ready(Ok(self.offset));
					},
					std::task::Poll::Ready(Err(error)) => {
						return std::task::Poll::Ready(Err(std::io::Error::other(error)));
					},
					std::task::Poll::Pending => {
						return std::task::Poll::Pending;
					},
				},
				None => {
					return std::task::Poll::Ready(Ok(self.offset));
				},
			}
		}
	}
}

impl Server {
	pub async fn handle_get_build_log_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "log"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.wrap_err("Failed to deserialize the search params.")?
			.unwrap_or_default();

		// Get the accept header.
		let accept = request
			.headers()
			.get(http::header::ACCEPT)
			.map(|accept| {
				let accept = accept.to_str().wrap_err("Invalid content type.")?;
				let accept = accept
					.parse::<mime::Mime>()
					.wrap_err("Invalid content type.")?;
				Ok::<_, Error>(accept)
			})
			.transpose()?;
		let Some(accept) = accept else {
			return Err(error!("The accept header must be set."));
		};

		// Get the log.
		let Some(log) = self.try_get_build_log(&id, arg, None).await? else {
			return Ok(not_found());
		};

		// Choose the content type.
		let content_type = match (accept.type_(), accept.subtype()) {
			(mime::TEXT, mime::EVENT_STREAM) => mime::TEXT_EVENT_STREAM,
			_ => return Err(error!("Invalid accept header.")),
		};

		// Create the body.
		let body = log
			.map_ok(|chunk| {
				let data = serde_json::to_string(&chunk).unwrap();
				let event = tangram_util::sse::Event::with_data(data);
				hyper::body::Frame::data(event.to_string().into())
			})
			.map_err(Into::into);
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
			return Err(error!("Unexpected path."));
		};
		let build_id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();

		self.add_build_log(user.as_ref(), &build_id, bytes).await?;

		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}
}

fn poll_read_impl(
	server: &Server,
	id: &tg::build::Id,
	offset: u64,
	limit: u64,
	mut row_offset: u64,
) -> BoxFuture<'static, Result<Option<ReadSuccess>>> {
	let server = server.clone();
	let id = id.clone();
	let future = async move {
		// Get the chunk that overlaps the current position of the reader.
		let db = server.inner.database.get().await?;
		let statement = "
			select offset, data
			from build_logs
			where
				build = ?1 and
				(
					(offset <= ?2 and offset + length(data) > ?2) or
					(offset > ?2 and offset < ?3)
				)
			order by offset
		";
		let params = params![id.to_string(), offset, offset + limit];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare statement.")?;
		let mut rows = statement
			.query(params)
			.wrap_err("Failed to perform query.")?;

		let mut data = Vec::with_capacity(limit.to_usize().unwrap());
		while let Some(row) = rows.next().wrap_err("Failed to get a row.")? {
			let chunk_offset = row.get::<_, u64>(0).unwrap();
			let chunk_data = row.get::<_, Vec<u8>>(1).unwrap();
			if chunk_offset < offset {
				let start = (offset - chunk_offset).to_usize().unwrap();
				data.extend_from_slice(&chunk_data[start..]);
			} else {
				data.extend_from_slice(&chunk_data);
			}
			row_offset += 1;
		}

		let data = Bytes::from(data);
		Ok(Some(ReadSuccess { data, row_offset }))
	};
	future.boxed()
}

fn poll_seek_impl(
	server: &Server,
	id: &tg::build::Id,
	offset: u64,
	position: std::io::SeekFrom,
) -> BoxFuture<'static, Result<SeekSuccess>> {
	let server = server.clone();
	let id = id.clone();
	let future = async move {
		// Get the new position of the reader.
		let offset = match position {
			std::io::SeekFrom::Start(position) => position,
			std::io::SeekFrom::Current(position) => {
				if position < 0 {
					offset.saturating_sub(position.abs().to_u64().unwrap())
				} else {
					offset.saturating_add(position.to_u64().unwrap())
				}
			},
			std::io::SeekFrom::End(position) => {
				let db = server.inner.database.get().await?;
				let statement = "
					select coalesce (max(offset) + length(data), 0)
					from build_logs
					where
					build = ?1
				";
				let params = params![id.to_string()];
				let mut statement = db
					.prepare_cached(statement)
					.wrap_err("Failed to prepare statement.")?;
				let mut query = statement.query(params).wrap_err("Failed to make query.")?;
				let end = query
					.next()
					.wrap_err("Failed to get row.")?
					.wrap_err("Expected a row.")?
					.get::<_, u64>(0)
					.wrap_err("Failed to get position.")?;
				if position < 0 {
					end.saturating_sub(position.abs().to_u64().unwrap())
				} else {
					end.saturating_add(position.to_u64().unwrap())
				}
			},
		};

		// Get the row offset that corresponds to `offset`.
		let db = server.inner.database.get().await?;
		let statement = "
			select coalesce(
				(
					select rowid
					from build_logs
					where
						build = ?1 and
						offset <= ?2 and
						offset + length(data) > ?2
				),
				(
					select max(rowid)
					from build_logs
					order by offset
				)
			);
		";
		let id = id.to_string();
		let params = params![id, offset];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare statement.")?;
		let mut query = statement.query(params).wrap_err("Failed to make query.")?;
		let row_offset = query
			.next()
			.wrap_err("Failed to get row.")?
			.wrap_err("Expected a row.")?
			.get(0)
			.wrap_err("Expected a row id.")?;
		Ok(SeekSuccess { offset, row_offset })
	};
	future.boxed()
}
