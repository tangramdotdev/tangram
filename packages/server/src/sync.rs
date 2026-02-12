use {
	crate::{Context, Server, sync::graph::Graph},
	futures::{prelude::*, stream::BoxStream},
	num::ToPrimitive as _,
	std::{
		panic::AssertUnwindSafe,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::{
		read::Ext as _,
		stream::Ext as _,
		task::{Stop, Task},
		write::Ext as _,
	},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tokio::io::AsyncReadExt as _,
	tokio_stream::wrappers::ReceiverStream,
	tracing::Instrument,
};

mod get;
mod graph;
mod progress;
mod put;
mod queue;

impl Server {
	#[tracing::instrument(fields(get = ?arg.get, put = ?arg.put), level = "trace", name = "sync", skip_all)]
	pub(crate) async fn sync_with_context(
		&self,
		context: &Context,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + use<>> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the remote client"))?;
			let arg = tg::sync::Arg {
				commands: arg.commands,
				errors: arg.errors,
				eager: arg.eager,
				force: arg.force,
				get: arg.get,
				local: None,
				logs: arg.logs,
				metadata: arg.metadata,
				outputs: arg.outputs,
				put: arg.put,
				recursive: arg.recursive,
				remotes: None,
			};
			let stream = client
				.sync(arg, stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to sync with remote"))?;
			return Ok(stream.boxed());
		}

		// Guard against concurrent cleans.
		let _clean_guard = self.try_acquire_clean_guard()?;

		// Create the task.
		let (sender, receiver) = tokio::sync::mpsc::channel(4096);
		let task = Task::spawn({
			let server = self.clone();
			let context = context.clone();
			|_| {
				async move {
					let result =
						AssertUnwindSafe(server.sync_task(&context, arg, stream, sender.clone()))
							.catch_unwind()
							.instrument(tracing::Span::current())
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
			}
		});

		let stream = ReceiverStream::new(receiver);
		let stream = stream
			.take_while_inclusive(|message| {
				future::ready(!matches!(message, Err(_) | Ok(tg::sync::Message::End)))
			})
			.attach(task);

		Ok(stream.boxed())
	}

	async fn sync_task(
		&self,
		context: &Context,
		arg: tg::sync::Arg,
		mut stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		// Create the graph.
		let graph = Arc::new(Mutex::new(Graph::new(&arg.get, &arg.put)));

		// Spawn the input task to receive the input.
		let (get_input_sender, get_input_receiver) =
			tokio::sync::mpsc::channel::<tg::sync::PutMessage>(256);
		let (put_input_sender, put_input_receiver) =
			tokio::sync::mpsc::channel::<tg::sync::GetMessage>(256);
		let mut input_task = Task::spawn(|_| async move {
			while let Some(message) = stream.try_next().await? {
				match message {
					tg::sync::Message::Get(message) => {
						put_input_sender.send(message).await.ok();
					},
					tg::sync::Message::Put(message) => {
						get_input_sender.send(message).await.ok();
					},
					tg::sync::Message::End => {
						tracing::trace!("received end");
						return Ok(());
					},
				}
			}
			Ok::<_, tg::Error>(())
		});
		input_task.detach();

		// Create the output future to send the output.
		let (get_output_sender, get_output_receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::sync::GetMessage>>(256);
		let (put_output_sender, put_output_receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::sync::PutMessage>>(256);
		let output_future = async move {
			let mut stream = stream::select(
				ReceiverStream::new(get_output_receiver).map_ok(tg::sync::Message::Get),
				ReceiverStream::new(put_output_receiver).map_ok(tg::sync::Message::Put),
			)
			.chain(stream::once(future::ok(tg::sync::Message::End)))
			.take_while_inclusive(|result| future::ready(result.is_ok()));
			while let Some(result) = stream.next().await {
				sender
					.send(result)
					.await
					.map_err(|_| tg::error!("failed to send the message"))?;
			}
			Ok::<_, tg::Error>(())
		};

		// Create the get future.
		let get_future = {
			let server = self.clone();
			let arg = arg.clone();
			let context = context.clone();
			let graph = graph.clone();
			let stream = ReceiverStream::new(get_input_receiver).boxed();
			async move {
				server
					.sync_get(arg, context, graph, stream, get_output_sender)
					.instrument(tracing::debug_span!("get"))
					.await
			}
		};

		// Create the put future.
		let put_future = {
			let server = self.clone();
			let arg = arg.clone();
			let graph = graph.clone();
			let stream = ReceiverStream::new(put_input_receiver).boxed();
			async move {
				server
					.sync_put(arg, graph, stream, put_output_sender)
					.instrument(tracing::debug_span!("put"))
					.await
			}
		};

		// Await the futures.
		future::try_join4(
			input_task
				.wait()
				.map_err(|source| tg::error!(!source, "the input task panicked"))
				.and_then(future::ready),
			output_future,
			get_future,
			put_future,
		)
		.await?;

		Ok(())
	}

	pub(crate) async fn handle_sync_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

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
				tg::sync::PutItemMessage::Object(tg::sync::PutItemObjectMessage {
					id, bytes, ..
				}),
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

		let stream = self
			.sync_with_context(context, arg, stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the sync"))?;

		// Validate the accept header.
		let sync_content_type: mime::Mime = tg::sync::CONTENT_TYPE.parse().unwrap();
		match accept.as_ref() {
			None => (),
			Some(accept) if accept.type_() == mime::STAR && accept.subtype() == mime::STAR => (),
			Some(accept) if *accept == sync_content_type => (),
			Some(accept) => {
				let type_ = accept.type_();
				let subtype = accept.subtype();
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		// Create the response body.
		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);
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
					let json = serde_json::to_string(&error.to_data_or_id()).unwrap();
					trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
					hyper::body::Frame::trailers(trailers)
				},
			};
			Ok::<_, tg::Error>(frame)
		});
		let body = BoxBody::with_stream(stream);

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
