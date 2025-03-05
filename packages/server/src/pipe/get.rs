use crate::Server;
use futures::{future, stream, Stream, StreamExt as _, TryStreamExt};
use tangram_client::{self as tg};
use tangram_either::Either;
use tangram_futures::{stream::Ext, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::{self as messenger, Messenger as _};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

impl Server {
	pub async fn get_pipe_stream(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::get::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let stream = remote.get_pipe_stream(id, arg).await?.boxed();
			return Ok(stream);
		}

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
		let events = tokio::spawn(async move {
			let mut stream = std::pin::pin!(stream);
			while let Some(event) = stream.next().await {
				send.send(event).await.ok();
			}
		});
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
			.subscribe(format!("pipes.{id}"), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the pipe message"))?
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
		let message_batch_size = 256;
		let message_batch_timeout = std::time::Duration::from_secs(1);

		let stream = messenger
			.jetstream
			.get_stream(format!("pipes.{id}"))
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream"))?;
		// Get the consumer.
		let consumer_config = async_nats::jetstream::consumer::pull::Config {
			durable_name: Some(format!("pipes.{id}")),
			..Default::default()
		};
		let consumer = stream
			.get_or_create_consumer(&format!("pipes.{id}"), consumer_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the index consumer"))?;

		// Create the stream.
		let stream = stream::try_unfold(consumer, |consumer| async {
			let mut batch = consumer
				.batch()
				.max_messages(message_batch_size)
				.expires(message_batch_timeout)
				.messages()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the batch"))?;
			let mut messages = Vec::new();
			while let Some(message) = batch.try_next().await? {
				let (message, acker) = message.split();
				let result = serde_json::from_slice::<tg::pipe::Event>(&message.payload);
				acker.ack().await?;
				let message = match result {
					Ok(message) => Ok::<_, tg::Error>(message),
					Err(source) => Err(tg::error!(!source, "failed to deserialize the payload")),
				};
				messages.push((message, Some(acker)));
			}
			if messages.is_empty() {
				return Ok::<_, tg::Error>(None);
			}
			Ok(Some((messages, consumer)))
		});
		Ok(stream::empty())
	}
}

impl Server {
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
