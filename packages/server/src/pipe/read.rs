use crate::Server;
use futures::{FutureExt, Stream, StreamExt as _, TryStreamExt as _, future, stream};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::task::Stop;
use tangram_http::{Body, request::Ext as _};
use tangram_messenger as messenger;

impl Server {
	pub async fn read_pipe(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::read::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static> {
		// Forward to a remote if requested.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let stream = remote.read_pipe(id, arg).await?.boxed();
			return Ok(stream);
		}

		// Create the stream from the messenger.
		let stream = match &self.messenger {
			Either::Left(messenger) => self
				.read_pipe_memory(messenger, id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get pipe stream"))?
				.left_stream(),
			Either::Right(messenger) => self
				.read_pipe_nats(messenger, id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get pipe stream"))?
				.right_stream(),
		};

		let deleted = self.pipe_deleted(id.clone());
		Ok(stream
			.take_until(deleted)
			.chain(stream::once(future::ok(tg::pipe::Event::End)))
			.boxed())
	}

	async fn read_pipe_memory(
		&self,
		messenger: &messenger::memory::Messenger,
		id: &tg::pipe::Id,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static> {
		let stream = messenger
			.streams()
			.subscribe(id.to_string())
			.await
			.map_err(|source| tg::error!(!source, "the pipe was closed or does not exist"))?
			.map(|message| {
				serde_json::from_slice::<tg::pipe::Event>(&message.payload)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))
			})
			.boxed();
		Ok(stream)
	}

	async fn read_pipe_nats(
		&self,
		messenger: &messenger::nats::Messenger,
		id: &tg::pipe::Id,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static> {
		let stream = messenger
			.jetstream
			.get_stream(id.to_string())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream"))?;

		// Get the consumer.
		let name = tg::Id::new_uuidv7(tg::id::Kind::Pipe);
		let consumer_config = async_nats::jetstream::consumer::pull::Config {
			durable_name: Some(name.to_string()),
			..Default::default()
		};
		let consumer = stream
			.get_or_create_consumer(&id.to_string(), consumer_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the index consumer"))?;

		// Create the stream.
		let stream = consumer
			.stream()
			.messages()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream"))?
			.map_err(|source| tg::error!(!source, "failed to get message"))
			.and_then(|message| async move {
				message
					.ack()
					.await
					.map_err(|source| tg::error!(!source, "failed to ack message"))?;
				let event = serde_json::from_slice::<tg::pipe::Event>(&message.payload)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok::<_, tg::Error>(event)
			});

		Ok(stream)
	}
}

impl Server {
	pub(crate) async fn handle_read_pipe_request<H>(
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

		// Get the stream.
		let stream = handle.read_pipe(&id, arg).await?;

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let stream = stream.take_until(stop);

		// Create the body.
		let body = Body::with_stream(stream.map(move |result| {
			let event = match result {
				Ok(event) => match event {
					tg::pipe::Event::Chunk(bytes) => hyper::body::Frame::data(bytes),
					tg::pipe::Event::End => {
						let mut trailers = http::HeaderMap::new();
						trailers.insert("x-tg-event", http::HeaderValue::from_static("end"));
						hyper::body::Frame::trailers(trailers)
					},
				},
				Err(error) => {
					let mut trailers = http::HeaderMap::new();
					trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
					let json = serde_json::to_string(&error).unwrap();
					trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
					hyper::body::Frame::trailers(trailers)
				},
			};
			Ok::<_, tg::Error>(event)
		}));

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
