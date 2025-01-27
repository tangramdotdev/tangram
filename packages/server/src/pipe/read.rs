use crate::Server;
use futures::{Stream, StreamExt as _};
use http_body_util::StreamBody;
use tangram_client as tg;
use tangram_futures::task::Stop;
use tangram_http::{Incoming, Outgoing};
use tangram_messenger::Messenger as _;

impl Server {
	pub async fn read_pipe(
		&self,
		id: &tg::pipe::Id,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::read::Event>> + Send + 'static> {
		let subject = format!("pipes.{id}");
		let stream = self
			.messenger
			.subscribe(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.boxed();
		let stream = stream
			.map(|message| {
				if message.payload.is_empty() {
					tg::pipe::read::Event::End
				} else {
					tg::pipe::read::Event::Chunk(message.payload)
				}
			})
			.map(Ok);
		Ok(stream)
	}
}

impl Server {
	pub(crate) async fn handle_read_pipe_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the stream.
		let stream = handle.read_pipe(&id).await?;

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let stream = stream.take_until(stop);

		// Create the body.
		let body = Outgoing::body(StreamBody::new(stream.map(|result| match result {
			Ok(event) => match event {
				tg::pipe::read::Event::Chunk(bytes) => Ok(hyper::body::Frame::data(bytes)),
				tg::pipe::read::Event::End => {
					let mut trailers = http::HeaderMap::new();
					trailers.insert("x-tg-event", http::HeaderValue::from_static("end"));
					Ok(hyper::body::Frame::trailers(trailers))
				},
			},
			Err(error) => {
				let mut trailers = http::HeaderMap::new();
				trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
				let json = serde_json::to_string(&error).unwrap();
				trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
				Ok(hyper::body::Frame::trailers(trailers))
			},
		})));

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
