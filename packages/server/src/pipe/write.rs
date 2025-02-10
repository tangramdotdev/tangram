use crate::Server;
use futures::{stream::TryStreamExt as _, Stream, StreamExt as _};
use http_body_util::{BodyExt as _, BodyStream};
use tangram_client as tg;
use tangram_http::{response::builder::Ext as _, Body};

impl Server {
	pub async fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		stream: impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static,
	) -> tg::Result<()> {
		let sender = self
			.pipes
			.get(id)
			.ok_or_else(|| tg::error!("failed to find the pipe"))?
			.value()
			.sender
			.clone();

		let mut stream = std::pin::pin!(stream);
		while let Some(event) = stream.try_next().await? {
			sender
				.send(event)
				.await
				.map_err(|source| tg::error!(!source, "failed to write to the pipe"))?;
		}
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_write_pipe_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

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
			.boxed();

		// Write.
		handle.write_pipe(&id, stream).await?;

		// Create the response.
		let response = http::Response::builder().empty().unwrap();

		Ok(response)
	}
}
