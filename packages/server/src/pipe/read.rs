use crate::Server;
use futures::{Stream, StreamExt as _, TryStreamExt as _, future};
use tangram_client as tg;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::Messenger as _;

impl Server {
	pub async fn read_pipe(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::read::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let stream = remote.read_pipe(id, arg).await?;
			return Ok(stream.left_stream());
		}

		// Create the stream.
		let stream = id.to_string();
		let consumer_name = uuid::Uuid::now_v7().to_string();
		let stream = self
			.messenger
			.stream_subscribe(stream, consumer_name)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the stream"))?
			.map_err(|source| tg::error!(!source, "failed to get a message"))
			.and_then(|message| async move {
				message
					.acker
					.ack()
					.await
					.map_err(|source| tg::error!(!source, "failed to ack the message"))?;
				let event = serde_json::from_slice::<tg::pipe::Event>(&message.payload)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok::<_, tg::Error>(event)
			})
			.take_while_inclusive(|result| {
				future::ready(!matches!(result, Ok(tg::pipe::Event::End) | Err(_)))
			});

		Ok(stream.right_stream())
	}

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
		let stop = async move {
			stop.wait().await;
		};
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
