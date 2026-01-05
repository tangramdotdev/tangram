use {
	crate::{Context, Server},
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive,
	std::{io::SeekFrom, time::Duration},
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
		mut arg: tg::process::log::get::Arg,
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
		'outer: loop {
			// Get the process's status.
			let status = self
				.get_current_process_status_local(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the process status"))?;

			// Create and drain the stream.
			let mut stream = self
				.process_log_stream(id, arg.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to create the log stream"))?;
			while let Some(chunk) = stream.next().await {
				if let Ok(chunk) = &chunk {
					// Update position/length
					arg.position.replace(SeekFrom::Start(chunk.position + chunk.bytes.len().to_u64().unwrap()));
					if let Some(length) = &mut arg.length {
						*length = length.signum()
							* length
								.abs()
								.to_u64()
								.unwrap()
								.saturating_sub(chunk.bytes.len().to_u64().unwrap())
								.to_i64()
								.unwrap();
					}
				}
				let event = chunk.map(tg::process::log::get::Event::Chunk);

				// If we fail to send the event it means the connection has closed, so break the outer loop.
				if sender.send(event).await.is_err() {
					break 'outer;
				}
			}

			// Send the finished event if we've reached the end of the stream.
			if status.is_finished() || arg.length.is_some_and(|l| l == 0) {
				sender
					.send(Ok(tg::process::log::get::Event::End))
					.await
					.ok();
				break;
			}

			// Otherwise wait for an event before returning to the top of the loop.
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
