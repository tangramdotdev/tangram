use {
	crate::{Context, Server},
	futures::{Stream, StreamExt as _},
	std::os::fd::AsFd as _,
	tangram_client::prelude::*,
	tangram_futures::task::Stop,
	tangram_http::{Body, request::Ext as _},
	tokio_util::io::ReaderStream,
};

impl Server {
	pub async fn read_pipe_with_context(
		&self,
		_context: &Context,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + use<>> {
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self.get_remote_client(remote).await?;
			let arg = tg::pipe::read::Arg {
				local: None,
				remotes: None,
			};
			let stream = client.read_pipe(id, arg).await?;
			return Ok(stream.left_stream());
		}

		let pipe = self
			.pipes
			.get(id)
			.ok_or_else(|| tg::error!("failed to find the pipe"))?;
		let fd = pipe
			.receiver
			.as_fd()
			.try_clone_to_owned()
			.map_err(|source| tg::error!(!source, "failed to clone the receiver"))?;
		let receiver = tokio::net::unix::pipe::Receiver::from_owned_fd_unchecked(fd)
			.map_err(|source| tg::error!(!source, "failed to clone the receiver"))?;

		let stream = ReaderStream::new(receiver)
			.map(|result| match result {
				Ok(bytes) if bytes.is_empty() => Ok(tg::pipe::Event::End),
				Ok(bytes) => Ok(tg::pipe::Event::Chunk(bytes)),
				Err(source) => Err(tg::error!(!source, "failed to read pipe")),
			})
			.right_stream();

		Ok(stream)
	}

	pub(crate) async fn handle_read_pipe_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Parse the ID.
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the stream.
		let stream = self.read_pipe_with_context(context, &id, arg).await?;

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
					let json = serde_json::to_string(&error.to_data()).unwrap();
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
