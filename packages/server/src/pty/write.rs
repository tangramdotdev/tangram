use crate::Server;
use futures::{Stream, StreamExt as _, stream::TryStreamExt as _};
use http_body_util::{BodyExt as _, BodyStream};
use std::pin::pin;
use tangram_client as tg;
use tangram_futures::task::Stop;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn write_pty(
		&self,
		id: &tg::pty::Id,
		mut arg: tg::pty::write::Arg,
		stream: impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static,
	) -> tg::Result<()> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			return remote.write_pty(id, arg, stream.boxed()).await;
		}
		let deleted = self.pty_deleted(id.clone());
		let mut stream = pin!(stream.take_until(deleted));
		while let Some(event) = stream.try_next().await? {
			if let Err(error) = self.send_pty_event(id, event, arg.master).await {
				tracing::error!(?error, %id, "failed to write pty");
				break;
			}
		}
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_write_pty_request<H>(
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
		let stop = async move {
			stop.wait().await;
		};

		// Create the stream.
		let body = request
			.into_body()
			.map_err(|source| tg::error!(!source, "failed to read the body"));

		let stream = BodyStream::new(body)
			.and_then(|frame| async {
				match frame.into_data() {
					Ok(bytes) => Ok(tg::pty::Event::Chunk(bytes)),
					Err(frame) => {
						let trailers = frame.into_trailers().unwrap();
						let event = trailers
							.get("x-tg-event")
							.ok_or_else(|| tg::error!("missing event"))?
							.to_str()
							.map_err(|source| tg::error!(!source, "invalid event"))?;
						match event {
							"window-size" => {
								let data = trailers
									.get("x-tg-data")
									.ok_or_else(|| tg::error!("missing data"))?
									.to_str()
									.map_err(|source| tg::error!(!source, "invalid data"))?;
								let window_size = serde_json::from_str(data).map_err(|source| {
									tg::error!(!source, "failed to deserialize the header value")
								})?;
								Ok(tg::pty::Event::Size(window_size))
							},
							"end" => Ok(tg::pty::Event::End),
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
			.take_until(stop)
			.boxed();

		// Send the stream.
		handle.write_pty(&id, arg, stream).await?;

		// Create the response.
		let response = http::Response::builder().empty().unwrap();

		Ok(response)
	}
}
