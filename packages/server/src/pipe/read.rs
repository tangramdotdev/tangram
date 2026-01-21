use {
	crate::{Context, Server},
	futures::{FutureExt as _, Stream, StreamExt as _, future, stream},
	std::os::fd::AsFd as _,
	tangram_client::prelude::*,
	tangram_futures::task::Stop,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tokio_util::io::ReaderStream,
};

impl Server {
	pub async fn try_read_pipe_with_context(
		&self,
		_context: &Context,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + use<>>> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(stream) = self
				.try_read_pipe_local(id)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to read the pipe"))?
		{
			return Ok(Some(stream.left_stream()));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(stream) = self
			.try_read_pipe_remote(id, arg.clone(), &remotes)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to read the pipe from the remote"))?
		{
			return Ok(Some(stream.right_stream()));
		}

		Ok(None)
	}

	async fn try_read_pipe_local(
		&self,
		id: &tg::pipe::Id,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + use<>>> {
		let Some(pipe) = self.pipes.get(id) else {
			return Ok(None);
		};
		let fd = pipe
			.receiver
			.as_fd()
			.try_clone_to_owned()
			.map_err(|source| tg::error!(!source, "failed to clone the receiver"))?;
		let receiver = tokio::net::unix::pipe::Receiver::from_owned_fd_unchecked(fd)
			.map_err(|source| tg::error!(!source, "failed to clone the receiver"))?;

		let stream = ReaderStream::new(receiver).map(|result| match result {
			Ok(bytes) => Ok(tg::pipe::Event::Chunk(bytes)),
			Err(source) => Err(tg::error!(!source, "failed to read pipe")),
		})
		.chain(stream::once(future::ok(tg::pipe::Event::End)));

		Ok(Some(stream))
	}

	async fn try_read_pipe_remote(
		&self,
		id: &tg::pipe::Id,
		_arg: tg::pipe::read::Arg,
		remotes: &[String],
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + use<>>> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let arg = tg::pipe::read::Arg {
			local: None,
			remotes: None,
		};
		let futures = remotes.iter().map(|remote| {
			let remote = remote.clone();
			let arg = arg.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				client
					.try_read_pipe_stream(id, arg)
					.await
					.map_err(|source| tg::error!(!source, %remote, "failed to read the pipe"))?
					.ok_or_else(|| tg::error!("not found"))
					.map(futures::StreamExt::boxed)
			}
			.boxed()
		});
		let Ok((stream, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		Ok(Some(stream))
	}

	pub(crate) async fn handle_read_pipe_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Parse the ID.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the pipe id"))?;

		// Get the query.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the stream.
		let Some(stream) = self.try_read_pipe_with_context(context, &id, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

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
					let json = serde_json::to_string(&error.to_data_or_id()).unwrap();
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
