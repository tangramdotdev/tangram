/*
get
	input task
		on put item
			remove get
			if eager
				send to index task
			else
				enqueue children
			send to store task
		on missing
			remove get
			if eager
				send to index task
			else
				error

	queue task
		touch and get complete and metadata
		update graph with complete and metadata
		if not present, the send a get
		if not complete, then enqueue children
		if complete, then send complete

	index task
		touch and get complete
		update graph with complete and metadata
		if complete
			send complete
		if missing
			if not present
				error
			if not complete
				enqueue children

	store task
		store
		update graph with stored
		update progress

put
	input task
		on get item
			enqueue the item
		on complete
			update graph

	queue task
		update graph
		if !inserted || complete
			send to index task
		else
			send to store task

	index task
		get metadata
		update progress

	store task
		get item
		send item or missing
		if eager
			enqueue children
		update graph to mark as stored
		update progress
*/

use {
	crate::{Context, Server},
	futures::{prelude::*, stream::BoxStream},
	num::ToPrimitive as _,
	std::{panic::AssertUnwindSafe, pin::pin},
	tangram_client::prelude::*,
	tangram_futures::{read::Ext as _, stream::Ext as _, task::Stop, write::Ext},
	tangram_http::{Body, request::Ext as _},
	tokio::io::AsyncReadExt as _,
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::task::AbortOnDropHandle,
	tracing::Instrument,
};

mod get;
mod progress;
mod put;
mod queue;

impl Server {
	#[tracing::instrument(fields(get = ?arg.get, put = ?arg.put), name = "sync", skip_all)]
	pub(crate) async fn sync_with_context(
		&self,
		_context: &Context,
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
			.instrument(tracing::Span::current())
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
		// Create the future to distribute the input.
		let (get_input_sender, get_input_receiver) =
			tokio::sync::mpsc::channel::<tg::sync::PutMessage>(256);
		let (put_input_sender, put_input_receiver) =
			tokio::sync::mpsc::channel::<tg::sync::GetMessage>(256);
		let input_future = async move {
			while let Some(message) = stream.try_next().await? {
				tracing::trace!(?message, "received message");
				match message {
					tg::sync::Message::Get(message) => {
						put_input_sender.send(message).await.ok();
					},
					tg::sync::Message::Put(message) => {
						get_input_sender.send(message).await.ok();
					},
					tg::sync::Message::End => {
						break;
					},
				}
			}
			Ok::<_, tg::Error>(())
		};

		// Create a future to collect the output.
		let (get_output_sender, get_output_receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::sync::GetMessage>>(256);
		let (put_output_sender, put_output_receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::sync::PutMessage>>(256);
		let output_future = {
			let sender = sender.clone();
			async move {
				let mut stream = stream::select(
					ReceiverStream::new(get_output_receiver).map_ok(tg::sync::Message::Get),
					ReceiverStream::new(put_output_receiver).map_ok(tg::sync::Message::Put),
				);
				while let Some(result) = stream.next().await {
					tracing::trace!(?result, "sending message");
					sender.send(result).await.ok();
				}
			}
		};

		let get_future = {
			let server = self.clone();
			let arg = arg.clone();
			let stream = ReceiverStream::new(get_input_receiver).boxed();
			async move { server.sync_get(arg, stream, get_output_sender).await }
		}
		.instrument(tracing::debug_span!("get"));

		let put_future = {
			let server = self.clone();
			let arg = arg.clone();
			let stream = ReceiverStream::new(put_input_receiver).boxed();
			async move { server.sync_put(arg, stream, put_output_sender).await }
		}
		.instrument(tracing::debug_span!("put"));

		match future::try_select(
			pin!(future::try_join3(
				get_future,
				put_future,
				output_future.map(Ok)
			)),
			pin!(input_future),
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

	pub(crate) async fn handle_sync_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
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
			if let tg::sync::Message::Put(tg::sync::PutMessage::Item(
				tg::sync::PutItemMessage::Object(tg::sync::PutItemObjectMessage { id, bytes }),
			)) = &message
			{
				let actual = tg::object::Id::new(id.kind(), bytes);
				if id != &actual {
					return Err(tg::error!(expected = %id, %actual, "invalid object id"));
				}
			}

			Ok(Some((message, reader)))
		})
		.boxed();

		let stream = self.sync_with_context(context, arg, stream).await?;

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
