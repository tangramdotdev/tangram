use crate::Server;
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use tangram_client::{self as tg};
use tangram_either::Either;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::{self as messenger, Messenger as _};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

impl Server {
	pub async fn get_pipe_window_size(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::get::Arg,
	) -> tg::Result<Option<tg::pipe::WindowSize>> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			return remote.get_pipe_window_size(id, arg).await;
		}

		let pipe = self
			.try_get_pipe(id)
			.await?
			.ok_or_else(|| tg::error!("pipe was closed or destroyed"))?;

		eprintln!("GET pipes/{id}/window: {pipe:#?}");
		Ok(pipe.window_size)
	}

	pub async fn get_pipe_stream(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::get::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static> {
		// Forward to a remote if requested.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let stream = remote.get_pipe_stream(id, arg).await?.boxed();
			return Ok(stream);
		}

		// Create a timer to continuously check for the pipe.
		let (send, recv) = tokio::sync::mpsc::channel(8);
		let timer = tokio::spawn({
			let send = send.clone();
			let server = self.clone();
			let id = id.clone();
			async move {
				let mut timer = tokio::time::interval(std::time::Duration::from_millis(100));
				while let Ok(Some(pipe)) = server.try_get_pipe(&id).await {
					timer.tick().await;
					if pipe.closed {
						break;
					}
				}
				send.send(Ok::<_, tg::Error>(tg::pipe::Event::End))
					.await
					.ok();
			}
		});

		// Create the stream from the messenger.
		let stream = match &self.messenger {
			Either::Left(messenger) => self
				.get_pipe_stream_memory(messenger, id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get pipe stream"))?
				.left_stream(),
			Either::Right(messenger) => self
				.get_pipe_stream_nats(messenger, id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get pipe stream"))?
				.right_stream(),
		};

		// Merge with the timer.
		let id = id.clone();
		let events = tokio::spawn(async move {
			let mut stream = std::pin::pin!(stream);
			while let Some(event) = stream.next().await {
				eprintln!("GET {id}: {event:?}");
				send.send(event).await.ok();
			}
		});

		// Create the stream with abort handlers.
		let stream = ReceiverStream::new(recv)
			.attach(AbortOnDropHandle::new(timer))
			.attach(AbortOnDropHandle::new(events));

		Ok(stream.boxed())
	}

	async fn get_pipe_stream_memory(
		&self,
		messenger: &messenger::memory::Messenger,
		id: &tg::pipe::Id,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static> {
		let stream = messenger
			.subscribe(id.to_string(), None)
			.await
			.map_err(|source| tg::error!(!source, "the pipe was closed or does not exist"))?
			.map(|message| {
				let event = serde_json::from_slice::<tg::pipe::Event>(&message.payload)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"));
				event
			})
			.boxed();
		Ok(stream)
	}

	async fn get_pipe_stream_nats(
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
		let consumer_config = async_nats::jetstream::consumer::pull::Config {
			durable_name: Some(id.to_string()),
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
	pub(crate) async fn handle_get_pipe_window_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;
		let window_size = handle.get_pipe_window_size(&id, arg).await?;
		let response = http::Response::builder().json(window_size).unwrap();
		Ok(response)
	}

	pub(crate) async fn handle_get_pipe_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;
		eprintln!("GET /pipes/{id}");

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the stream.
		let stream = handle.get_pipe_stream(&id, arg).await?;

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let stream = stream.take_until(stop);

		// Create the body.
		let body = Body::with_stream(stream.map(move |result| {
			let event = match result {
				Ok(event) => match event {
					tg::pipe::Event::Chunk(bytes) => hyper::body::Frame::data(bytes),
					tg::pipe::Event::WindowSize(window_size) => {
						let mut trailers = http::HeaderMap::new();
						trailers
							.insert("x-tg-event", http::HeaderValue::from_static("window-size"));
						let json = serde_json::to_string(&window_size).unwrap();
						trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
						hyper::body::Frame::trailers(trailers)
					},
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
