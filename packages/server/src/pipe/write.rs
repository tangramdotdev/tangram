use crate::Server;
use futures::{Stream, StreamExt as _, future, stream::TryStreamExt as _};
use http_body_util::{BodyExt as _, BodyStream};
use std::pin::pin;
use tangram_client as tg;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::write::Arg,
		stream: impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static,
	) -> tg::Result<()> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			return remote.write_pipe(id, arg, stream.boxed()).await;
		}

		let deleted = self.pipe_deleted(id.clone());
		let mut stream = pin!(stream.take_until(deleted));
		while let Some(event) = stream.try_next().await? {
			if let Err(error) = self.send_pipe_event(id, event).await {
				tracing::error!(?error, %id, "failed to write pipe");
				break;
			}
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

		// Send the stream.
		handle.write_pipe(&id, arg, stream).await?;

		// Create the response.
		let response = http::Response::builder().empty().unwrap();

		Ok(response)
	}
}
