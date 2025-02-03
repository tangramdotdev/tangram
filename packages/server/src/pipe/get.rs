use crate::Server;
use futures::{Stream, StreamExt as _};
use tangram_client as tg;
use tangram_futures::{stream::Ext, task::Stop};
use tangram_http::{request::Ext as _, Body};
use tangram_messenger::Messenger;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

use super::Pipe;

impl Server {
	pub async fn get_pipe_stream(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::get::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let stream = remote.get_pipe_stream(id, arg).await?.left_stream();
			return Ok(stream);
		}
		let Pipe { writer, .. } = self
			.try_get_pipe(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the pipe"))?
			.ok_or_else(|| tg::error!(%id, "missing pipe"))?;

		let (send, recv) = tokio::sync::mpsc::channel(8);
		let timer = tokio::spawn({
			let send = send.clone();
			let server = self.clone();
			let id = id.clone();
			async move {
				let mut timer = tokio::time::interval(std::time::Duration::from_millis(100));
				while let Ok(Some(pipe)) = server.try_get_pipe(&id).await {
					timer.tick().await;
					if pipe.closed {
						break;
					}
				}
				send.send(Ok::<_, tg::Error>(tg::pipe::Event::End))
					.await
					.ok();
			}
		});
		let stream = self
			.messenger
			.subscribe(format!("pipes.{writer}"), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the pipe message"))?
			.map(|message| {
				let event = serde_json::from_slice::<tg::pipe::Event>(&message.payload)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"));
				event
			})
			.boxed();
		let events = tokio::spawn(async move {
			let mut stream = std::pin::pin!(stream);
			while let Some(event) = stream.next().await {
				send.send(event).await.ok();
			}
		});
		let stream = ReceiverStream::new(recv)
			.attach(AbortOnDropHandle::new(timer))
			.attach(AbortOnDropHandle::new(events));
		Ok(stream.right_stream())
	}
}

impl Server {
	pub(crate) async fn handle_get_pipe_request<H>(
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
		let stream = handle.get_pipe_stream(&id, arg).await?;

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let stream = stream.take_until(stop);

		// Create the body.
		let body = Body::with_stream(stream.map(move |result| {
			let event = match result {
				Ok(event) => match event {
					tg::pipe::Event::Chunk(bytes) => hyper::body::Frame::data(bytes),
					tg::pipe::Event::WindowSize(window_size) => {
						let mut trailers = http::HeaderMap::new();
						trailers
							.insert("x-tg-event", http::HeaderValue::from_static("window-size"));
						let json = serde_json::to_string(&window_size).unwrap();
						trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
						hyper::body::Frame::trailers(trailers)
					},
					tg::pipe::Event::End => {
						let mut trailers = http::HeaderMap::new();
						trailers.insert("x-tg-event", http::HeaderValue::from_static("end"));
						hyper::body::Frame::trailers(trailers)
					},
				},
				Err(error) => {
					let mut trailers = http::HeaderMap::new();
					trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
					let json = serde_json::to_string(&error).unwrap();
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
