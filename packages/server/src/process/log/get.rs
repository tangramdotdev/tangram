use {
	super::reader::Reader,
	crate::{Context, Server},
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_futures::{
		stream::Ext as _,
		task::{Stop, Task},
	},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
	tokio::io::{AsyncReadExt as _, AsyncSeekExt as _},
	tokio_stream::wrappers::IntervalStream,
};

impl Server {
	pub async fn try_get_process_log_stream_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> tg::Result<
		Option<
			impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static + use<>,
		>,
	> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(stream) = self
				.try_get_process_log_local(id, arg.clone())
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the process log"))?
		{
			return Ok(Some(stream.left_stream()));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(stream) = self
			.try_get_process_log_remote(id, arg.clone(), &remotes)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process log from remote"))?
		{
			return Ok(Some(stream.right_stream()));
		}

		Ok(None)
	}

	async fn try_get_process_log_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> tg::Result<
		Option<
			impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static + use<>,
		>,
	> {
		// Verify the process is local.
		if !self
			.get_process_exists_local(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to check if the process exists"))?
		{
			return Ok(None);
		}

		// Create the channel.
		let (sender, receiver) = async_channel::unbounded();

		// Spawn the task.
		let server = self.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = server
				.try_get_process_log_local_task(&id, arg, sender.clone())
				.await;
			if let Err(error) = result {
				sender.try_send(Err(error)).ok();
			}
		});

		let stream = receiver.attach(task);

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
			.subscribe::<()>(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();

		// Subscribe to status events.
		let subject = format!("processes.{id}.status");
		let status = self
			.messenger
			.subscribe::<()>(subject, None)
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
		let mut reader = Reader::new(self, id, arg.stream).await?;

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
				.get_current_process_status_local(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the process status"))?;

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
		remotes: &[String],
	) -> tg::Result<
		Option<
			impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static + use<>,
		>,
	> {
		// Attempt to get the process log from the remotes.
		if remotes.is_empty() {
			return Ok(None);
		}
		let arg = tg::process::log::get::Arg {
			local: None,
			remotes: None,
			..arg
		};
		let futures = remotes.iter().map(|remote| {
			let remote = remote.clone();
			let arg = arg.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				client
					.get_process_log(id, arg)
					.await
					.map(futures::StreamExt::boxed)
					.map_err(
						|source| tg::error!(!source, %id, %remote, "failed to get the process log"),
					)
			}
			.boxed()
		});
		let Ok((stream, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		let stream = stream
			.map_ok(tg::process::log::get::Event::Chunk)
			.chain(stream::once(future::ok(tg::process::log::get::Event::End)));
		Ok(Some(stream))
	}

	pub(crate) async fn handle_get_process_log_request(
		&self,
		request: http::Request<Body>,
		_context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Parse the ID.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the query.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the stream.
		let Some(stream) = self.try_get_process_log_stream(&id, arg).await? else {
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
}
