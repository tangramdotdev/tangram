use {
	crate::{Server, handle::ServerOrProxy},
	futures::{Stream, StreamExt as _, future, stream::TryStreamExt as _},
	http_body_util::{BodyExt as _, BodyStream},
	std::pin::pin,
	tangram_client::{self as tg, prelude::*},
	tangram_futures::{stream::Ext as _, task::Stop},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tokio::io::AsyncWriteExt as _,
};

impl Server {
	pub async fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::write::Arg,
		stream: impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			return remote.write_pipe(id, arg, stream.boxed()).await;
		}

		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			let mut pipe = self
				.pipes
				.get_mut(id)
				.ok_or_else(|| tg::error!("failed to find the pipe"))?;
			match event {
				tg::pipe::Event::Chunk(chunk) => {
					pipe.sender
						.as_mut()
						.ok_or_else(|| tg::error!("the pipe is closed"))?
						.write_all(&chunk)
						.await
						.map_err(|source| tg::error!(!source, "failed to write to the pipe"))?;
				},
				tg::pipe::Event::End => {
					return Err(tg::error!("cannot write an end event"));
				},
			}
		}

		Ok(())
	}

	pub(crate) async fn handle_write_pipe_request(
		handle: &ServerOrProxy,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Parse the ID.
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };

		// Create the stream.
		let body = request
			.into_body()
			.map_err(|source| tg::error!(!source, "failed to read the body"));
		let stream = BodyStream::new(body)
			.and_then(|frame| async {
				match frame.into_data() {
					Ok(bytes) => Ok(tg::pipe::Event::Chunk(bytes)),
					Err(frame) => {
						let trailers = frame.into_trailers().unwrap();
						let event = trailers
							.get("x-tg-event")
							.ok_or_else(|| tg::error!("missing event"))?
							.to_str()
							.map_err(|source| tg::error!(!source, "invalid event"))?;
						match event {
							"end" => Ok(tg::pipe::Event::End),
							"error" => {
								let data = trailers
									.get("x-tg-data")
									.ok_or_else(|| tg::error!("missing data"))?
									.to_str()
									.map_err(|source| tg::error!(!source, "invalid data"))?;
								let error = serde_json::from_str(data).map_err(|source| {
									tg::error!(!source, "failed to deserialize the header value")
								})?;
								Err(error)
							},
							_ => Err(tg::error!("invalid event")),
						}
					},
				}
			})
			.take_while_inclusive(|event| future::ready(!matches!(event, Ok(tg::pipe::Event::End))))
			.take_until(stop)
			.boxed();

		handle.write_pipe(&id, arg, stream).await?;

		// Create the response.
		let response = http::Response::builder().empty().unwrap();

		Ok(response)
	}
}
