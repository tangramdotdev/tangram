use {
	futures::{FutureExt as _, future::BoxFuture, prelude::*, stream::BoxStream},
	std::{
		ops::ControlFlow,
		panic::AssertUnwindSafe,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::{Session, messenger::Messenger, sync::InnerArg},
	tokio_stream::wrappers::ReceiverStream,
	tracing::Instrument,
};

mod get;
mod graph;
mod progress;
mod put;
mod queue;

pub(crate) mod service;

pub(crate) use self::graph::Graph;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub(crate) clients: tangram_messenger::service::Clients<self::service::Protocol, Messenger>,
	pub(crate) index_tasks: tangram_futures::task::Set<tg::Result<()>>,
	pub(crate) service_tasks: tangram_futures::task::Set<tg::Result<()>>,
}

impl Component {
	pub fn new(arg: &tangram_server_core::component::Arg<'_>) -> tg::Result<Self> {
		// Create the sync clients, which observe their events as checkpoints.
		let observer = Arc::new(self::service::Observer::new(arg.checkpoints.cloned()));
		let clients = tangram_messenger::service::Clients::new(
			arg.messenger.clone(),
			"syncs.service",
			arg.config.sync.service,
			observer,
		)
		.map_err(|source| tg::error!(!source, "failed to create the sync clients"))?;

		// Create the state.
		let state = State {
			clients,
			index_tasks: tangram_futures::task::Set::default(),
			service_tasks: tangram_futures::task::Set::default(),
		};
		let component = Self(Arc::new(state));

		Ok(component)
	}
}

impl Component {
	#[tracing::instrument(fields(get_count = arg.get.len(), put_count = arg.put.len()), level = "trace", name = "sync", skip_all)]
	pub(crate) async fn sync(
		&self,
		session: &Session,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::sync::Header,
				BoxStream<'static, tg::Result<tg::sync::Message>>,
			),
			tg::Error,
		>,
	> {
		let arg = InnerArg {
			arg,
			..Default::default()
		};
		self.sync_inner(session, arg, stream).await
	}

	pub async fn sync_inner(
		&self,
		session: &Session,
		arg: InnerArg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::sync::Header,
				BoxStream<'static, tg::Result<tg::sync::Message>>,
			),
			tg::Error,
		>,
	> {
		let InnerArg {
			arg,
			get,
			process,
			trust,
		} = arg;
		let location = session.server.location(arg.location.as_ref())?;

		let output = match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				if get.is_some() {
					return Err(tg::error!("additional get nodes require a local sync"));
				}
				self.sync_region(session, arg, process, stream, region)
					.await?
			},
			tg::Location::Local(_) => {
				let (header, stream) = self.sync_local(session, arg, get, stream, trust).await?;
				let stream = stream.with_stopper(session.context.stopper.clone()).boxed();
				ControlFlow::Break((header, stream))
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				if get.is_some() {
					return Err(tg::error!("additional get nodes require a local sync"));
				}
				self.sync_remote(session, arg, process, stream, remote, region)
					.await?
			},
		};

		Ok(output)
	}

	async fn sync_local(
		&self,
		session: &Session,
		mut arg: tg::sync::Arg,
		get: Option<tokio::sync::mpsc::Receiver<tg::Referent<tg::Selector<tg::Id>>>>,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		trust: bool,
	) -> tg::Result<(
		tg::sync::Header,
		BoxStream<'static, tg::Result<tg::sync::Message>>,
	)> {
		// Join the command sync of the connect that a nonce names, giving the requested nodes the sync's tokens.
		let sync = match arg.sync.take() {
			Some(tg::sync::Join::Connect(nonce)) => {
				let sync = session
					.server
					.components
					.process
					.try_resolve_process_command_sync(session, &nonce)
					.await?
					.ok_or_else(|| tg::error!("failed to find the command sync of the connect"))?;
				Self::inherit_sync_tokens(&mut arg.get, &sync);
				Some(sync)
			},
			Some(tg::sync::Join::Process(process)) => {
				let sync = session
					.server
					.components
					.process
					.try_resolve_process_control_sync(session, &process.id, process.kind)
					.await?
					.ok_or_else(
						|| tg::error!(process = %process.id, "failed to find the sync of the process"),
					)?;
				Self::inherit_sync_tokens(&mut arg.get, &sync);
				Some(sync)
			},
			Some(tg::sync::Join::Sync(sync)) => Some(sync),
			None => None,
		};

		// Verify or create the sync before starting the transfer.
		let sync = Self::prepare_sync(session, sync)?;
		arg.sync = Some(tg::sync::Join::Sync(sync.clone()));
		let header = tg::sync::Header { sync: Some(sync) };

		// Start the transfer.
		let capacity = session.server.config.sync.channel_capacity;
		let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
		let task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
			|_| {
				async move {
					let future = AssertUnwindSafe(component.sync_task(
						&session,
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

	/// Give the requested nodes the tokens of the sync that they join.
	fn inherit_sync_tokens(
		get: &mut [tg::Referent<tg::Selector<tg::Id>>],
		sync: &tg::Referent<tg::sync::Id>,
	) {
		for referent in get {
			for token in sync.options.tokens.local_authorization() {
				referent
					.options
					.tokens
					.insert_local_authorization(token.clone());
			}
		}
	}

	pub(crate) fn prepare_sync(
		session: &Session,
		sync: Option<tg::Referent<tg::sync::Id>>,
	) -> tg::Result<tg::Referent<tg::sync::Id>> {
		// Verify an existing sync, or create a new one.
		let id = if let Some(sync) = sync {
			let authorized = sync
				.options
				.tokens
				.local_authorization()
				.iter()
				.any(|token| {
					session
						.server
						.components
						.authorization
						.try_get_sync_id_from_token(session, token)
						.as_ref() == Some(&sync.node)
				});
			if !authorized {
				return Err(tg::error!("invalid sync authorization"));
			}
			sync.node
		} else {
			tg::sync::Id::new()
		};

		// Issue a fresh token for the sync, so that a new attempt of the sync holds an unexpired token.
		let sync = Self::refresh_sync(session, id)?;

		Ok(sync)
	}

	pub(crate) fn refresh_sync(
		session: &Session,
		id: tg::sync::Id,
	) -> tg::Result<tg::Referent<tg::sync::Id>> {
		let token = session
			.server
			.components
			.authorization
			.create_read_token(session, &id.clone().into())?;
		let sync = tg::Referent::with_node_and_local_tokens(id, token);
		Ok(sync)
	}

	async fn sync_region(
		&self,
		session: &Session,
		arg: tg::sync::Arg,
		process: bool,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		region: String,
	) -> tg::Result<
		ControlFlow<
			(
				tg::sync::Header,
				BoxStream<'static, tg::Result<tg::sync::Message>>,
			),
			tg::Error,
		>,
	> {
		let client = if process {
			session.get_region_session_for_process(&region).await
		} else {
			session.get_region_session(&region).await
		}
		.map_err(|error| tg::error!(!error, region = %region, "failed to get the region client"))?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::sync::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client
			.sync(arg, stream)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to sync"))?
			.map_break(|(header, stream)| (header, stream.boxed()))
			.map_continue(|error| tg::error!(!error, region = %region, "failed to sync"));
		Ok(output)
	}

	async fn sync_remote(
		&self,
		session: &Session,
		arg: tg::sync::Arg,
		process: bool,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::sync::Header,
				BoxStream<'static, tg::Result<tg::sync::Message>>,
			),
			tg::Error,
		>,
	> {
		let client = if process {
			session
				.server
				.components
				.remote
				.get_remote_session_for_process(session, &remote)
				.await
		} else {
			session
				.server
				.components
				.remote
				.get_remote_session(session, &remote)
				.await
		}
		.map_err(|error| tg::error!(!error, remote = %remote, "failed to get the remote client"))?;
		let arg = tg::sync::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let output = client
			.sync(arg, stream)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to sync"))?
			.map_break(|(header, stream)| (header, stream.boxed()))
			.map_continue(|error| tg::error!(!error, remote = %remote, "failed to sync"));
		Ok(output)
	}

	async fn sync_task(
		&self,
		session: &Session,
		arg: tg::sync::Arg,
		get: Option<tokio::sync::mpsc::Receiver<tg::Referent<tg::Selector<tg::Id>>>>,
		mut stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
		trust: bool,
	) -> tg::Result<()> {
		// Create the graph.
		let checkout_pointers = Self::sync_get_checkout_pointers_enabled(session);
		let mut graph = Graph::new(&arg, checkout_pointers);
		graph.set_get_open(true);
		let graph = Arc::new(Mutex::new(graph));

		// Spawn the input task to receive the input.
		let capacity = session.server.config.sync.message_channel_capacity;
		let (get_input_sender, get_input_receiver) =
			tokio::sync::mpsc::channel::<tg::sync::PutMessage>(capacity);
		let (put_input_sender, put_input_receiver) =
			tokio::sync::mpsc::channel::<tg::sync::GetMessage>(capacity);
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
			tokio::sync::mpsc::channel::<tg::Result<tg::sync::GetMessage>>(capacity);
		let (put_output_sender, put_output_receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::sync::PutMessage>>(capacity);
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
			let component = self.clone();
			let session = session.clone();
			let arg = arg.clone();
			let graph = graph.clone();
			let stream = ReceiverStream::new(get_input_receiver).boxed();
			let sender = get_output_sender.clone();
			async move {
				let arg = self::get::SyncGetArg {
					get,
					graph,
					sender: get_output_sender,
					stream,
					sync_arg: arg,
					trust,
				};
				let future = component
					.sync_get(&session, arg)
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
			let component = self.clone();
			let session = session.clone();
			let arg = arg.clone();
			let graph = graph.clone();
			let stream = ReceiverStream::new(put_input_receiver).boxed();
			let sender = put_output_sender.clone();
			async move {
				let result = Box::pin(
					component
						.sync_put(&session, arg, graph, stream, put_output_sender)
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
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Select the request encoding.
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the content type"))?
			.ok_or_else(|| tg::error!(argument, "missing the content type"))?;
		let input_encoding = tangram_server_core::http::frame::Encoding::from_content_type(
			&content_type,
			tg::sync::CONTENT_TYPE,
		)?;

		// Parse the arg.
		let (arg, request) = request
			.arg_with_tangram::<tg::sync::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Select the response encoding.
		let output_encoding = tangram_server_core::http::frame::Encoding::from_accept(
			accept.as_ref(),
			tg::sync::CONTENT_TYPE,
		)?;

		// Decode the request body.
		let max_frame_size = session.server.config.sync.max_frame_size;
		let stream =
			tangram_server_core::http::frame::decode(request, input_encoding, max_frame_size);

		// Start the sync, responding that the server is unavailable if the sync could not reach a server.
		let output = self
			.sync(session, arg, stream)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the sync"))?;
		let (header, stream) = match output {
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => {
				return Err(tg::error!(unavailable, !error, "failed to start the sync"));
			},
		};
		tangram_server_core::checkpoint!(session.server, "sync.request.response").await;

		// Create the response body.
		let content_type = output_encoding.content_type(tg::sync::CONTENT_TYPE);
		let body =
			tangram_server_core::http::frame::encode(stream, output_encoding, max_frame_size);
		let body = tangram_http::body::header::set(body, &header, output_encoding.serialization())
			.map_err(|error| tg::error!(!error, "failed to serialize the header"))?;

		// Create the response.
		let mut response = http::Response::builder();
		response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		let response = response.body(body).unwrap();

		Ok(response)
	}
}

impl Component {
	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["sync"]) => self.sync_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {
	fn stop<'a>(&'a self, _server: &'a tangram_server_core::Server) -> BoxFuture<'a, ()> {
		async move {
			// Enqueue interrupted transfers before stopping their control tasks.
			self.index_tasks.wait().await;

			// Stop the sync clients and drain the sync service tasks after the transfers have stopped.
			self.clients.stop();
			self.service_tasks.stop_all();
			self.service_tasks.wait().await;
		}
		.boxed()
	}

	fn abort(&self) {
		self.service_tasks.abort_all();
		self.index_tasks.abort_all();
	}
}

impl tangram_server_core::sync::Component for Component {
	fn prepare_sync<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		sync: Option<tg::Referent<tg::sync::Id>>,
	) -> tg::Result<tg::Referent<tg::sync::Id>> {
		Self::prepare_sync(session, sync)
	}

	fn refresh_sync<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: tg::sync::Id,
	) -> tg::Result<tg::Referent<tg::sync::Id>> {
		Self::refresh_sync(session, id)
	}

	fn request_sync_verification<'a>(
		&'a self,
		sync: &'a tg::sync::Id,
		arg: tg::sync::service::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::sync::service::Output>>> {
		self.request_sync_verification(sync, arg).boxed()
	}

	fn sync_inner<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: InnerArg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				(
					tg::sync::Header,
					BoxStream<'static, tg::Result<tg::sync::Message>>,
				),
				tg::Error,
			>,
		>,
	> {
		self.sync_inner(session, arg, stream).boxed()
	}

	fn sync<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				(
					tg::sync::Header,
					BoxStream<'static, tg::Result<tg::sync::Message>>,
				),
				tg::Error,
			>,
		>,
	> {
		self.sync(session, arg, stream).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
