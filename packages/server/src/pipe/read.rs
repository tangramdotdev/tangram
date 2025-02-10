use crate::Server;
use futures::{Stream, StreamExt as _};
use tangram_client as tg;
use tangram_futures::task::Stop;
use tangram_http::Body;
use tokio_stream::wrappers::ReceiverStream;

impl Server {
	pub async fn read_pipe(
		&self,
		id: &tg::pipe::Id,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static> {
		let receiver = self
			.pipes
			.get_mut(id)
			.ok_or_else(|| tg::error!("failed to find the pipe"))?
			.receiver
			.take()
			.ok_or_else(|| tg::error!("failed to get the pipe"))?;
		let stream = ReceiverStream::new(receiver).map(Ok);
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

		// Get the stream.
		let stream = handle.read_pipe(&id).await?;

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
