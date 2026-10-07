use {
	crate::Session,
	futures::{prelude::*, stream::BoxStream},
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

pub(crate) use self::graph::Graph;

pub(crate) mod control;

#[derive(Default)]
pub(crate) struct InnerArg {
	pub arg: tg::sync::Arg,
	pub get: Option<tokio::sync::mpsc::Receiver<tg::Referent<tg::Selector<tg::Id>>>>,
	pub process: bool,
	pub trust: bool,
}

impl Session {
	#[tracing::instrument(fields(get_count = arg.get.len(), put_count = arg.put.len()), level = "trace", name = "sync", skip_all)]
	pub(crate) async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<(
		tg::sync::Header,
		impl Stream<Item = tg::Result<tg::sync::Message>> + Send + use<>,
	)> {
		let arg = InnerArg {
			arg,
			..Default::default()
		};
		self.sync_inner(arg, stream).await
	}

	pub(crate) async fn sync_inner(
		&self,
		arg: InnerArg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<(
		tg::sync::Header,
		BoxStream<'static, tg::Result<tg::sync::Message>>,
	)> {
		let InnerArg {
			arg,
			get,
			process,
			trust,
		} = arg;
		let location = self.server.location(arg.location.as_ref())?;

		let (header, stream) = match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != self.server.config.region.as_deref() => {
				if get.is_some() {
					return Err(tg::error!("additional get nodes require a local sync"));
				}
				self.sync_region(arg, process, stream, region).await?
			},
			tg::Location::Local(_) => {
				let (header, stream) = self.sync_local(arg, get, stream, trust).await?;
				let stream = stream.with_stopper(self.context.stopper.clone());
				(header, stream)
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				if get.is_some() {
					return Err(tg::error!("additional get nodes require a local sync"));
				}
				self.sync_remote(arg, process, stream, remote, region)
					.await?
			},
		};

		Ok((header, stream))
	}

	async fn sync_local(
		&self,
		mut arg: tg::sync::Arg,
		get: Option<tokio::sync::mpsc::Receiver<tg::Referent<tg::Selector<tg::Id>>>>,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		trust: bool,
	) -> tg::Result<(
		tg::sync::Header,
		BoxStream<'static, tg::Result<tg::sync::Message>>,
	)> {
		// Verify or create the sync before starting the transfer.
		arg.sync = Some(self.prepare_sync(arg.sync)?);
		let header = tg::sync::Header {
			sync: arg.sync.clone(),
		};

		// Start the transfer.
		let (sender, receiver) = tokio::sync::mpsc::channel(4096);
		let task = Task::spawn({
			let session = self.clone();
			|_| {
				async move {
					let future = AssertUnwindSafe(session.sync_task(
						arg,
						get,
						stream,
						sender.clone(),
						trust,
					))
					.catch_unwind()
					.instrument(tracing::Span::current());
					let result = future.boxed().await;
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

		Ok((header, stream.boxed()))
	}

	pub(crate) fn prepare_sync(
		&self,
		sync: Option<tg::Referent<tg::sync::Id>>,
	) -> tg::Result<tg::Referent<tg::sync::Id>> {
		if let Some(sync) = sync {
			let authorized = sync
				.options
				.tokens
				.local_authorization()
				.iter()
				.any(|token| self.try_get_sync_id_from_token(token).as_ref() == Some(&sync.node));
			if !authorized {
				return Err(tg::error!("invalid sync authorization"));
			}
			return Ok(sync);
		}
		let id = tg::sync::Id::new();
		let token = self.create_read_token(&id.clone().into())?;
		let sync = tg::Referent::with_node_and_local_tokens(id, token);
		Ok(sync)
	}

	async fn sync_region(
		&self,
		arg: tg::sync::Arg,
		process: bool,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		region: String,
	) -> tg::Result<(
		tg::sync::Header,
		BoxStream<'static, tg::Result<tg::sync::Message>>,
	)> {
		let client = if process {
			self.get_region_session_for_process(&region).await
		} else {
			self.get_region_session(&region).await
		}
		.map_err(|error| tg::error!(!error, region = %region, "failed to get the region client"))?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::sync::Arg {
			location: Some(location.into()),
			..arg
		};
		let (header, stream) = client
			.sync(arg, stream)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to sync"))?;
		Ok((header, stream.boxed()))
	}

	async fn sync_remote(
		&self,
		arg: tg::sync::Arg,
		process: bool,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<(
		tg::sync::Header,
		BoxStream<'static, tg::Result<tg::sync::Message>>,
	)> {
		let client = if process {
			self.get_remote_session_for_process(&remote).await
		} else {
			self.get_remote_session(&remote).await
		}
		.map_err(|error| tg::error!(!error, remote = %remote, "failed to get the remote client"))?;
		let arg = tg::sync::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let (header, stream) = client
			.sync(arg, stream)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to sync"))?;
		Ok((header, stream.boxed()))
	}

	async fn sync_task(
		&self,
		arg: tg::sync::Arg,
		get: Option<tokio::sync::mpsc::Receiver<tg::Referent<tg::Selector<tg::Id>>>>,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
		trust: bool,
	) -> tg::Result<()> {
		let mut session = self.clone();
		session.sync = arg.sync.as_ref().map(|sync| sync.node.clone());
		session.sync_control = Some(Arc::new(control::Client::default()));
		session
			.sync_task_inner(arg, get, stream, sender, trust)
			.await?;
		Ok(())
	}

	async fn sync_task_inner(
		&self,
		arg: tg::sync::Arg,
		get: Option<tokio::sync::mpsc::Receiver<tg::Referent<tg::Selector<tg::Id>>>>,
		mut stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
		trust: bool,
	) -> tg::Result<()> {
		// Create the graph.
		let checkout_pointers = self.sync_get_checkout_pointers_enabled();
		let mut graph = Graph::new(&arg, checkout_pointers);
		graph.set_get_open(true);
		let graph = Arc::new(Mutex::new(graph));

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
			let sender = get_output_sender.clone();
			async move {
				let future = session
					.sync_get(arg, get, graph, stream, get_output_sender, trust)
					.instrument(tracing::debug_span!("get"));
				match future.boxed().await {
					Ok(()) => Ok(()),
					Err(error) => {
						sender.send(Err(error.clone())).await.ok();
						Err(error)
					},
				}
			}
		};

		// Create the put future.
		let put_future = {
			let session = self.clone();
			let arg = arg.clone();
			let graph = graph.clone();
			let stream = ReceiverStream::new(put_input_receiver).boxed();
			let sender = put_output_sender.clone();
			async move {
				let result = Box::pin(
					session
						.sync_put(arg, graph, stream, put_output_sender)
						.instrument(tracing::debug_span!("put")),
				)
				.await;
				match result {
					Ok(()) => Ok(()),
					Err(error) => {
						sender.send(Err(error.clone())).await.ok();
						Err(error)
					},
				}
			}
		};

		// Await the futures.
		let future = future::try_join4(
			input_task
				.wait()
				.map_err(|error| tg::error!(!error, "the input task panicked"))
				.and_then(future::ready),
			output_future,
			get_future,
			put_future,
		);
		future.boxed().await?;

		Ok(())
	}

	pub(crate) async fn sync_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the arg.
		let (arg, request) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Create the request body.
		let reader = request.reader();
		let max_frame_size = self.server.config.sync.max_frame_size;
		let stream = stream::try_unfold(reader, move |mut reader| async move {
			let Some(len) = reader
				.try_read_uvarint()
				.await
				.map_err(|error| tg::error!(!error, "failed to read the length"))?
			else {
				return Ok(None);
			};
			if len > max_frame_size {
				return Err(tg::error!(
					argument,
					len = %len,
					max = %max_frame_size,
					"sync frame too large"
				));
			}
			let len = usize::try_from(len).map_err(
				|error| tg::error!(argument, !error, len = %len, "sync frame length out of range"),
			)?;
			let mut bytes = vec![0; len];
			reader
				.read_exact(&mut bytes)
				.await
				.map_err(|error| tg::error!(!error, "failed to read the message"))?;
			let message = tangram_serialize::from_slice(&bytes).map_err(|error| {
				tg::error!(argument, !error, "failed to deserialize the message")
			})?;
			Ok(Some((message, reader)))
		})
		.boxed();

		let (header, stream) = self
			.sync(arg, stream)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the sync"))?;
		crate::checkpoint!(self.server, "sync.request.response").await;

		// Validate the accept header.
		let sync_content_type: mime::Mime = tg::sync::CONTENT_TYPE.parse().unwrap();
		match accept.as_ref() {
			None => (),
			Some(accept) if accept.type_() == mime::STAR && accept.subtype() == mime::STAR => (),
			Some(accept) if *accept == sync_content_type => (),
			Some(accept) => {
				let type_ = accept.type_();
				let subtype = accept.subtype();
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		}

		// Create the response body.
		let content_type = Some(tg::sync::CONTENT_TYPE);
		let max_frame_size = self.server.config.sync.max_frame_size;
		let stream = stream.then(move |result| async move {
			let frame = match result {
				Ok(message) => {
					let message = tangram_serialize::to_vec(&message).unwrap();
					let message_len = message.len();
					let len = u64::try_from(message_len).map_err(
						|error| tg::error!(!error, len = %message_len, "sync frame length out of range"),
					)?;
					if len > max_frame_size {
						return Err(tg::error!(
							len = %len,
							max = %max_frame_size,
							"sync frame too large"
						));
					}
					let mut bytes = Vec::with_capacity(9 + message.len());
					bytes.write_uvarint(len).await.unwrap();
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
		let body = tangram_http::body::header::set(body, &header)
			.map_err(|error| tg::error!(!error, "failed to serialize the header"))?;

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
