use {
	crate::Server,
	futures::{Stream, StreamExt as _},
	tangram_client as tg,
	tangram_futures::{stream::Ext as _, task::Stop},
	tangram_http::{Body, request::Ext as _},
	tokio::io::AsyncReadExt as _,
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::task::AbortOnDropHandle,
};

impl Server {
	pub async fn read_pipe(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::read::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let stream = remote.read_pipe(id, arg).await?;
			return Ok(stream.left_stream());
		}
		let (send, recv) = tokio::sync::mpsc::channel(1);
		let pipe = self
			.pipes
			.get(id)
			.ok_or_else(|| tg::error!("could not find pipe"))?
			.read
			.try_clone()
			.map_err(|source| tg::error!(!source, "failed to clone pipe"))?;
		pipe.set_nonblocking(true)
			.map_err(|source| tg::error!(!source, "failed to set pipe as nonblocking"))?;
		let mut pipe = tokio::net::UnixStream::from_std(pipe)
			.map_err(|source| tg::error!(!source, "failed to create async pipe"))?;
		let task = AbortOnDropHandle::new(tokio::spawn({
			async move {
				loop {
					let mut buf = vec![0u8; 1024];
					match pipe.read(&mut buf).await {
						Ok(0) => {
							send.send(Ok(tg::pipe::Event::End)).await.ok();
							break;
						},
						Ok(n) => {
							buf.truncate(n);
							send.send(Ok(tg::pipe::Event::Chunk(buf.into()))).await.ok();
						},
						Err(source) => {
							send.send(Err(tg::error!(!source, "failed to read pipe")))
								.await
								.ok();
							break;
						},
					}
				}
			}
		}));
		let stream = ReceiverStream::new(recv).attach(task).right_stream();
		Ok(stream)
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
