use std::sync::atomic::{AtomicUsize, Ordering};

use crate::Server;
use bytes::Bytes;
use futures::{
	future,
	stream::{self, TryStreamExt as _},
	Stream, StreamExt as _,
};
use http_body_util::{BodyExt as _, BodyStream};
use std::pin::pin;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

pub struct Writer {
	pub(super) sender: async_channel::Sender<Bytes>,
	pub(super) refcount: AtomicUsize,
}

impl Writer {
	pub fn get_ref(&self) -> usize {
		self.refcount.load(Ordering::Relaxed)
	}

	pub fn add_ref(&self) -> usize {
		let mut current = self.get_ref();
		loop {
			if current == 0 {
				return 0;
			}
			match self.refcount.compare_exchange(
				current,
				current + 1,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => return current + 1,
				Err(new) => current = new,
			}
		}
	}

	pub fn release(&self) -> usize {
		let mut current = self.refcount.load(Ordering::Relaxed);
		loop {
			if current == 0 {
				return 0;
			}
			match self.refcount.compare_exchange(
				current,
				current - 1,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => return current - 1,
				Err(new) => current = new,
			}
		}
	}
}

impl Server {
	pub async fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
		stream: impl Stream<Item = tg::Result<Bytes>> + Send + 'static,
	) -> tg::Result<()> {
		if let Some(remote) = arg.remote {
			let remote = self.get_remote_client(remote.clone()).await?;
			return remote
				.write_pipe(id, tg::pipe::write::Arg::default(), stream.boxed())
				.await;
		}
		let pipe = self
			.pipes
			.get(id)
			.ok_or_else(|| tg::error!(%id, "missing pipe"))?;
		let writer = pipe
			.as_ref()
			.left()
			.ok_or_else(|| tg::error!(%id, "tried to write to a read pipe"))?;
		if writer.get_ref() == 0 {
			return Err(tg::error!(%id, "the pipe was closed"));
		}
		let sender = writer.sender.clone();
		drop(pipe);
		let mut stream = pin!(stream);
		while let Some(chunk) = stream.try_next().await? {
			sender
				.send(chunk)
				.await
				.map_err(|_| tg::error!(%id, "the pipe was closed"))?;
		}
		Ok(())
	}

	pub(crate) async fn write_pipe_bytes(
		&self,
		id: &tg::pipe::Id,
		remote: Option<String>,
		bytes: Bytes,
	) -> tg::Result<()> {
		let arg = tg::pipe::write::Arg { remote };
		self.write_pipe(id, arg, stream::once(future::ok(bytes)))
			.await?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_write_pipe_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
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
					Ok(bytes) => Ok(bytes),
					Err(frame) => {
						let trailers = frame.into_trailers().unwrap();
						let event = trailers
							.get("x-tg-event")
							.ok_or_else(|| tg::error!("missing event"))?
							.to_str()
							.map_err(|source| tg::error!(!source, "invalid event"))?;
						match event {
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
		handle.write_pipe(&id, arg, stream).await?;

		// Create the response.
		let response = http::Response::builder().empty().unwrap();

		Ok(response)
	}
}
