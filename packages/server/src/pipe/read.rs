use std::sync::atomic::{AtomicUsize, Ordering};

use crate::Server;
use bytes::Bytes;
use futures::{future, stream, Stream, StreamExt as _};
use http_body_util::StreamBody;
use tangram_client as tg;
use tangram_futures::task::Stop;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};

pub struct Reader {
	pub(super) receiver: async_channel::Receiver<Bytes>,
	pub(super) refcount: AtomicUsize,
}

impl Reader {
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
	pub async fn read_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static> {
		if let Some(remote) = arg.remote {
			let remote = self.get_remote_client(remote).await?;
			let stream = remote
				.read_pipe(id, tg::pipe::read::Arg::default())
				.await?
				.left_stream();
			return Ok(stream);
		}

		let pipe = self
			.pipes
			.get(id)
			.ok_or_else(|| tg::error!(%id, "pipe not found"))?;
		let reader = pipe
			.as_ref()
			.right()
			.ok_or_else(|| tg::error!(%id, "expected a read pipe"))?;
		if reader.get_ref() == 0 {
			return Err(tg::error!(%id, "pipe was closed"));
		}
		let receiver = reader.receiver.clone();
		drop(pipe);

		// Create a stream from the reader.
		let stream = stream::try_unfold((receiver,), |(receiver,)| async move {
			let Ok(bytes) = receiver.recv().await else {
				return Ok(None);
			};
			eprintln!("read bytes {bytes:?}");
			let event = tg::pipe::Event::Chunk(bytes);
			Ok::<_, tg::Error>(Some((event, (receiver,))))
		})
		.chain(stream::once(future::ok(tg::pipe::Event::End)));

		Ok(stream.right_stream())
	}
}

impl Server {
	pub(crate) async fn handle_read_pipe_request<H>(
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

		// Get the stream.
		let stream = handle.read_pipe(&id, arg).await?;

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let stream = stream.take_until(stop);

		// Create the body.
		let body = Outgoing::body(StreamBody::new(stream.map(move |result| match result {
			Ok(event) => match event {
				tg::pipe::Event::Chunk(bytes) => Ok(hyper::body::Frame::data(bytes)),
				tg::pipe::Event::End => {
					let mut trailers = http::HeaderMap::new();
					trailers.insert("x-tg-event", http::HeaderValue::from_static("end"));
					Ok(hyper::body::Frame::trailers(trailers))
				},
			},
			Err(error) => {
				let mut trailers = http::HeaderMap::new();
				trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
				let json = serde_json::to_string(&error).unwrap();
				trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
				Ok(hyper::body::Frame::trailers(trailers))
			},
		})));

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
