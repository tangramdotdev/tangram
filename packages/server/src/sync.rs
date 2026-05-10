use {
	crate::{Session, sync::graph::Graph},
	futures::{prelude::*, stream::BoxStream},
	num::ToPrimitive as _,
	std::{
		panic::AssertUnwindSafe,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::{read::Ext as _, stream::Ext as _, task::Task, write::Ext as _},
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

impl Session {
	#[tracing::instrument(fields(get = ?arg.get, put = ?arg.put), level = "trace", name = "sync", skip_all)]
	pub(crate) async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + use<>> {
		let location = self.server.location(arg.location.as_ref())?;

		let stream = match location {
			tg::Location::Local(tg::location::Local { region: None }) => self
				.sync_local(arg, stream)
				.await?
				.with_stopper(self.context.stopper.clone()),
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => self.sync_region(arg, stream, region).await?,
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => self.sync_remote(arg, stream, remote, region).await?,
		};

		Ok(stream)
	}

	async fn sync_local(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::sync::Message>>> {
		let (sender, receiver) = tokio::sync::mpsc::channel(4096);
		let task = Task::spawn({
			let session = self.clone();
			|_| {
				async move {
					let result = AssertUnwindSafe(session.sync_task(arg, stream, sender.clone()))
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

	async fn sync_region(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		region: String,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::sync::Message>>> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::sync::Arg {
			location: Some(location.into()),
			..arg
		};
		let stream = client
			.sync(arg, stream)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to sync"))?;
		Ok(stream.boxed())
	}

	async fn sync_remote(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::sync::Message>>> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::sync::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let stream = client
			.sync(arg, stream)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to sync"))?;
		Ok(stream.boxed())
	}

	async fn sync_task(
		&self,
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
			let session = self.clone();
			let arg = arg.clone();
			let graph = graph.clone();
			let stream = ReceiverStream::new(get_input_receiver).boxed();
			async move {
				session
					.sync_get(arg, graph, stream, get_output_sender)
					.instrument(tracing::debug_span!("get"))
					.await
			}
		};

		// Create the put future.
		let put_future = {
			let session = self.clone();
			let arg = arg.clone();
			let graph = graph.clone();
			let stream = ReceiverStream::new(put_input_receiver).boxed();
			async move {
				session
					.sync_put(arg, graph, stream, put_output_sender)
					.instrument(tracing::debug_span!("put"))
					.await
			}
		};

		// Await the futures.
		future::try_join4(
			input_task
				.wait()
				.map_err(|error| tg::error!(!error, "the input task panicked"))
				.and_then(future::ready),
			output_future,
			get_future,
			put_future,
		)
		.await?;

		Ok(())
	}

	pub(crate) async fn sync_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg_in_body = tangram_http::body::arg::get_header(request.headers())
			.map_err(|error| tg::error!(!error, "failed to parse the x-tg-arg-in-body header"))?;

		// Parse the arg.
		let arg = if arg_in_body {
			None
		} else {
			Some(
				request
					.query_params()
					.transpose()
					.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
					.unwrap_or_default(),
			)
		};

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Create the request body.
		let mut reader = request.reader();
		let arg = if let Some(arg) = arg {
			arg
		} else {
			tangram_http::body::arg::get(&mut reader)
				.await
				.map_err(|error| tg::error!(!error, "failed to read the sync arg"))?
		};
		let stream = stream::try_unfold(reader, move |mut reader| async move {
			let Some(len) = reader
				.try_read_uvarint()
				.await
				.map_err(|error| tg::error!(!error, "failed to read the length"))?
				.map(|value| value.to_usize().unwrap())
			else {
				return Ok(None);
			};
			let mut bytes = vec![0; len];
			reader
				.read_exact(&mut bytes)
				.await
				.map_err(|error| tg::error!(!error, "failed to read the message"))?;
			let message = tangram_serialize::from_slice(&bytes)
				.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
			Ok(Some((message, reader)))
		})
		.boxed();

		let stream = self
			.sync(arg, stream)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the sync"))?;

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
