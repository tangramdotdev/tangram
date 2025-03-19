use crate::Server;
use bytes::Bytes;
use futures::{
	Stream, StreamExt as _, future,
	stream::{self, TryStreamExt as _},
};
use http_body_util::{BodyExt as _, BodyStream};
use std::pin::pin;
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn post_pty(
		&self,
		id: &tg::pty::Id,
		mut arg: tg::pty::post::Arg,
		stream: impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static,
	) -> tg::Result<()> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			return remote.post_pty(id, arg, stream.boxed()).await;
		}
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			self.send_pty_event(id, event, arg.master).await?;
		}
		Ok(())
	}

	pub(crate) async fn write_pty_bytes(
		&self,
		id: &tg::pty::Id,
		remote: Option<String>,
		bytes: Bytes,
	) -> tg::Result<()> {
		let arg = tg::pty::post::Arg {
			remote,
			master: false,
		};
		self.post_pty(
			id,
			arg,
			stream::once(future::ok(tg::pty::Event::Chunk(bytes))),
		)
		.await?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_post_pty_request<H>(
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
								Ok(tg::pty::Event::WindowSize(window_size))
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
			.boxed();

		// Send the stream.
		handle.post_pty(&id, arg, stream).await?;

		// Create the response.
		let response = http::Response::builder().empty().unwrap();

		Ok(response)
	}
}
