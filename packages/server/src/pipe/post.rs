use super::Pipe;
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
	pub async fn post_pipe(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::post::Arg,
		stream: impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static,
	) -> tg::Result<()> {
		let _id = id.clone();
		if let Some(remote) = arg.remote.take() {
			eprintln!("POST stream {remote}.{id}");
			let remote = self.get_remote_client(remote.clone()).await?;
			return remote.post_pipe(id, arg, stream.boxed()).await;
		}
		let Pipe { reader, writer, .. } = self
			.try_get_pipe(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the pipe"))?
			.ok_or_else(|| tg::error!(%id, "missing pipe"))?;
		let pipe = if id == &reader { writer } else { reader };
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			eprintln!("POST {id}: {event:?}");
			self.send_pipe_event(&pipe, event).await?;
		}
		Ok(())
	}

	pub(crate) async fn write_pipe_bytes(
		&self,
		id: &tg::pipe::Id,
		remote: Option<String>,
		bytes: Bytes,
	) -> tg::Result<()> {
		let arg = tg::pipe::post::Arg { remote };
		self.post_pipe(
			id,
			arg,
			stream::once(future::ok(tg::pipe::Event::Chunk(bytes))),
		)
		.await?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_post_pipe_request<H>(
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
					Ok(bytes) => Ok(tg::pipe::Event::Chunk(bytes)),
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
								Ok(tg::pipe::Event::WindowSize(window_size))
							},
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

		// Send the stream.
		handle.post_pipe(&id, arg, stream).await?;

		// Create the response.
		let response = http::Response::builder().empty().unwrap();

		Ok(response)
	}
}
