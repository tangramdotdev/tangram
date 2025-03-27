use crate::Server;
use futures::{Stream, StreamExt as _, TryFutureExt, TryStreamExt as _, future};
use tangram_client as tg;
use tangram_futures::task::Stop;
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::Messenger as _;

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
		let name = tg::Id::new_uuidv7(tg::id::Kind::Pipe);
		let stream = self
			.messenger
			.stream_subscribe(id.to_string(), Some(name.to_string()))
			.await
			.map_err(|source| tg::error!(!source, "the pipe was closed or does not exist"))?
			.map_err(|source| tg::error!(!source, "stream error"))
			.and_then(|message| {
				future::ready({
					serde_json::from_slice::<tg::pipe::Event>(&message.payload)
						.map_err(|source| tg::error!(!source, "failed to deserialize the event"))
				})
			})
			.boxed();

		let deleted = self
			.pipe_deleted(id.clone())
			.inspect_err(|e| tracing::error!(?e, "failed to check if pipe was deleted"));

		Ok(stream.take_until(deleted).boxed())
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
