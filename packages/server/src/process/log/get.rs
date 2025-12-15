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
			&& let Some(stream) = self.try_get_process_log_local(id, arg.clone()).await?
		{
			return Ok(Some(stream.left_stream()));
		}

		// Try remotes.
		let remotes = self.remotes(arg.remotes.clone()).await?;
		if let Some(stream) = self
			.try_get_process_log_remote(id, arg.clone(), &remotes)
			.await?
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
		if !self.get_process_exists_local(id).await? {
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

		// Check if we're reading in reverse or not.
		let reverse = arg.length.is_some_and(|length| length < 0);

		// Create the state.
		let mut read = 0;
		let mut last_position = 0;
		loop {
			// Get the process's status.
			let status = self.get_current_process_status_local(id).await?;

			// Send as many chunks as possible.
			while let Some(chunk) = reader
				.next(reverse)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the next chunk"))?
			{
				read += chunk.bytes.len().to_u64().unwrap();
				last_position = chunk.position;
				let result = sender.try_send(Ok(tg::process::log::get::Event::Chunk(chunk)));
				if result.is_err() {
					return Ok(());
				}
				if arg
					.length
					.is_some_and(|length| length.abs().to_u64().unwrap() <= read)
					|| (reverse && last_position == 0)
				{
					break;
				}
			}

			// Check if we need to exit the loop or continue.
			if arg
				.length
				.is_some_and(|length| length.abs().to_u64().unwrap() <= read)
				|| status.is_finished()
				|| (reverse && last_position == 0)
			{
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
			async {
				let client = self.get_remote_client(remote.clone()).await?;
				client
					.get_process_log(id, arg.clone())
					.await
					.map(futures::StreamExt::boxed)
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
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

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
