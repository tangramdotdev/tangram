use {
	crate::Server,
	futures::{prelude::*, stream::BoxStream},
	num::ToPrimitive as _,
	std::{panic::AssertUnwindSafe, pin::pin},
	tangram_client as tg,
	tangram_futures::{read::Ext as _, stream::Ext as _, task::Stop, write::Ext},
	tangram_http::{Body, request::Ext as _},
	tokio::io::AsyncReadExt as _,
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::task::AbortOnDropHandle,
};

mod get;
mod put;

impl Server {
	pub async fn sync(
		&self,
		mut arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + use<>> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let client = self.get_remote_client(remote.clone()).await?;
			let stream = client.sync(arg, stream).await?;
			return Ok(stream.boxed());
		}

		// Create the task.
		let (sender, receiver) = tokio::sync::mpsc::channel(4096);
		let task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			async move {
				let result = AssertUnwindSafe(server.sync_inner(arg, stream, sender.clone()))
					.catch_unwind()
					.await;
				match result {
					Ok(Ok(())) => (),
					Ok(Err(error)) => {
						sender
							.send(Err(error))
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to send the error");
							})
							.ok();
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						sender
							.send(Err(tg::error!(?message, "the task panicked")))
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to send the panic");
							})
							.ok();
					},
				}
			}
		}));

		let stream = ReceiverStream::new(receiver);
		let stream = stream
			.take_while_inclusive(|message| {
				future::ready(!matches!(message, Err(_) | Ok(tg::sync::Message::End)))
			})
			.attach(task);

		Ok(stream.boxed())
	}

	async fn sync_inner(
		&self,
		arg: tg::sync::Arg,
		mut stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		let (get_sender, get_receiver) = tokio::sync::mpsc::channel::<tg::sync::Message>(256);
		let (put_sender, put_receiver) = tokio::sync::mpsc::channel::<tg::sync::Message>(256);

		let stream_future = async move {
			while let Some(message) = stream.try_next().await? {
				match message {
					tg::sync::Message::Get(_) | tg::sync::Message::Complete(_) => {
						put_sender.send(message).await.ok();
					},
					tg::sync::Message::Put(_) => {
						get_sender.send(message).await.ok();
					},
					_ => unreachable!(),
				}
			}
			Ok::<_, tg::Error>(())
		};

		let get_future = {
			let server = self.clone();
			let arg = arg.clone();
			let stream = ReceiverStream::new(get_receiver).boxed();
			let sender = sender.clone();
			async move { server.sync_get(arg, stream, sender).await }
		};

		let put_future = {
			let server = self.clone();
			let arg = arg.clone();
			let stream = ReceiverStream::new(put_receiver).boxed();
			let sender = sender.clone();
			async move { server.sync_put(arg, stream, sender).await }
		};

		match future::try_select(
			pin!(future::try_join(get_future, put_future)),
			pin!(stream_future),
		)
		.await
		.map_err(|error| error.factor_first().0)?
		{
			future::Either::Left(_) => (),
			future::Either::Right(((), future)) => {
				future.await?;
			},
		}

		sender
			.send(Ok(tg::sync::Message::End))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the end message"))?;

		Ok(())
	}

	pub(crate) async fn handle_sync_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the arg.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the stop signal.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();

		// Create the request body.
		let body = request.reader();
		let stream = stream::try_unfold(body, |mut reader| async move {
			// Read a message.
			let Some(len) = reader
				.try_read_uvarint()
				.await
				.map_err(|source| tg::error!(!source, "failed to read the length"))?
				.map(|value| value.to_usize().unwrap())
			else {
				return Ok(None);
			};
			let mut bytes = vec![0; len];
			reader
				.read_exact(&mut bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the message"))?;
			let message = tangram_serialize::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the message"))?;

			// Validate object IDs.
			if let tg::sync::Message::Put(Some(tg::sync::PutMessage::Object(message))) = &message {
				let actual = tg::object::Id::new(message.id.kind(), &message.bytes);
				if message.id != actual {
					return Err(tg::error!(%expected = message.id, %actual, "invalid object id"));
				}
			}

			Ok(Some((message, reader)))
		})
		.boxed();

		let stream = handle.sync(arg, stream).await?;

		// Create the response body.
		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);
		let (content_type, body) = if accept == Some(tg::sync::CONTENT_TYPE.parse().unwrap()) {
			let content_type = Some(tg::sync::CONTENT_TYPE);
			let stream = stream.then(|result| async {
				let frame = match result {
					Ok(message) => {
						let message = tangram_serialize::to_vec(&message).unwrap();
						let mut bytes = Vec::with_capacity(9 + message.len());
						bytes
							.write_uvarint(message.len().to_u64().unwrap())
							.await
							.unwrap();
						bytes.write_all(&message).await.unwrap();
						hyper::body::Frame::data(bytes.into())
					},
					Err(error) => {
						let mut trailers = http::HeaderMap::new();
						trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
						let json = serde_json::to_string(&error.to_data()).unwrap();
						trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
						hyper::body::Frame::trailers(trailers)
					},
				};
				Ok::<_, tg::Error>(frame)
			});
			let body = Body::with_stream(stream);
			(content_type, body)
		} else {
			return Err(tg::error!(?accept, "invalid accept header"));
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
