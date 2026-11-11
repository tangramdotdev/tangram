use {
	crate::Component,
	dashmap::DashSet,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream::BoxStream},
	num::ToPrimitive as _,
	std::{
		ops::ControlFlow,
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
		},
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
	tangram_messenger::Messenger as _,
	tangram_server_core::Session,
	tangram_server_sandbox_common::control::{Discarded, connected_subject, discarded_subject},
};

mod destroy;
mod stop;

pub(crate) mod service;

#[derive(Clone)]
pub(crate) struct ClientMessage(pub(crate) tg::sandbox::control::ClientMessage);

#[derive(Clone)]
pub(crate) struct ServerMessage(pub(crate) tg::sandbox::control::ServerMessage);

impl Component {
	pub async fn subscribe_sandbox_connection(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
	) -> tg::Result<super::ConnectionFuture> {
		let mut connected_stream = session
			.server
			.messenger
			.subscribe::<()>(connected_subject(id))
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					sandbox = %id,
					"failed to subscribe to the sandbox control connection"
				)
			})?;
		let mut discarded_stream = session
			.server
			.messenger
			.subscribe::<tangram_messenger::payload::Json<Discarded>>(discarded_subject(id))
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					sandbox = %id,
					"failed to subscribe to sandbox discard notifications"
				)
			})?;
		let id = id.clone();
		let future = async move {
			tokio::select! {
				result = connected_stream.try_next() => {
					result
						.map_err(|error| {
							tg::error!(
								!error,
								sandbox = %id,
								"failed to receive the sandbox control connection"
							)
						})?
						.ok_or_else(|| {
							tg::error!(
								sandbox = %id,
								"the sandbox control connection subscription ended"
							)
						})?;

					Ok(())
				},
				result = discarded_stream.try_next() => {
					let discarded = result
						.map_err(|error| {
							tg::error!(
								!error,
								sandbox = %id,
								"failed to receive a sandbox discard notification"
							)
						})?
						.ok_or_else(|| {
							tg::error!(
								sandbox = %id,
								"the sandbox discard notification subscription ended"
							)
						})?
						.payload
						.0;
					let error = tg::Error::try_from(discarded.error).map_err(|source| {
						tg::error!(
							!source,
							sandbox = %id,
							"failed to deserialize the sandbox discard error"
						)
					})?;

					Err(tg::error!(!error, sandbox = %id, "failed to create the sandbox"))
				},
			}
		}
		.boxed();

		Ok(future)
	}

	pub(crate) async fn get_sandbox_control_stream_with_context(
		&self,
		session: &Session,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::sandbox::control::Header,
				BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
			),
			tg::Error,
		>,
	> {
		let location = session.server.location(arg.location.as_ref())?;
		let output = match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				self.get_sandbox_control_stream_region(session, arg, stream, region)
					.await?
			},
			tg::Location::Local(_) => {
				let output = self
					.get_sandbox_control_stream_local(session, arg, stream)
					.await?;
				ControlFlow::Break(output)
			},
			tg::Location::Remote(tg::location::Remote { name, region }) => {
				self.get_sandbox_control_stream_remote(session, arg, stream, name, region)
					.await?
			},
		};
		Ok(output)
	}

	async fn get_sandbox_control_stream_local(
		&self,
		session: &Session,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<(
		tg::sandbox::control::Header,
		BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
	)> {
		self.get_sandbox_control_stream_local_inner(session, arg, stream)
			.boxed()
			.await
	}

	async fn get_sandbox_control_stream_local_inner(
		&self,
		session: &Session,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<(
		tg::sandbox::control::Header,
		BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
	)> {
		// Authenticate the connection as its sandbox, or as a runner that proves that it derived the sandbox's ID, which receives the sandbox's token.
		let id = arg
			.id
			.ok_or_else(|| tg::error!("a sandbox control connection requires a sandbox id"))?;
		let token = match (&session.context.principal, &arg.proof) {
			(tg::Principal::Sandbox(sandbox), _) if sandbox == &id => session.context.token.clone(),
			(tg::Principal::Runner(runner), Some(proof)) => {
				if arg.runner.as_ref() != Some(runner) {
					return Err(tg::error!(
						unauthorized,
						%runner,
						"a sandbox control connection's runner must be its principal"
					));
				}
				let now = session.server.clock.unix_timestamp();
				let now = now.to_u64().unwrap_or_default() * 1000;
				let window = session.server.config.sandbox.id_proof_window;
				proof.verify_recent(&id, runner, now, window)?;
				let token = session
					.server
					.components
					.authentication
					.create_sandbox_authentication_token(&session.server, id.clone())?;
				Some(token)
			},
			(tg::Principal::Sandbox(sandbox), _) => {
				return Err(tg::error!(sandbox = %sandbox, %id, "invalid sandbox"));
			},
			_ => return Err(tg::error!(unauthorized, "unauthorized")),
		};
		let context = tangram_server_core::context::Context {
			principal: tg::Principal::Sandbox(id.clone()),
			token: token.clone(),
			..session.context.clone()
		};
		let session = session.server.session(&context);
		let create = arg
			.create
			.ok_or_else(|| tg::error!("a sandbox control connection requires a create"))?;

		// Answer the requests for the sandbox for as long as the connection lasts, starting before initialization so that the requests that verify the runner do not wait for it.
		let service = self.spawn_sandbox_control_service_task(&session, &id, arg.runner.clone());
		if arg.proof.is_some() {
			tangram_server_core::checkpoint!(session.server, "sandbox.control.shortcut.received", sandbox = %id)
				.await;
		}

		let created_at = if let Some(created_at) = arg.created_at {
			created_at
		} else {
			session.server.clock.unix_timestamp()
		};
		let boot = arg.boot;
		let runner = arg.runner;

		// Prepare and submit initialization before accepting subsequent requests.
		tangram_server_core::checkpoint!(session.server, "sandbox.control.connect", sandbox = %id)
			.await;
		let create = tg::process::control::SandboxCreate {
			arg: create,
			created_at,
			creator: arg.creator,
			owner: arg.owner,
			runner: runner.clone(),
		};
		let mut sandbox = self
			.prepare_sandbox_control_index_arg(&session, &id, create)
			.await?;
		if let Some(data) = &mut sandbox.data {
			data.data.boot = boot;
		}

		// Remove the boot from its scheduler's queue in the same write as the connection's status, because the scheduler stops owning the sandbox once its runner connects.
		let delete = tangram_index::scheduler::sandbox::delete::Arg {
			boot,
			sandbox: id.clone(),
		};
		let arg = tangram_index::write::Arg {
			items: vec![
				tangram_index::Item::PutSandbox(Box::new(sandbox)),
				tangram_index::Item::DeleteSchedulerSandbox(Box::new(delete)),
			],
		};
		session
			.server
			.components
			.indexer
			.write(&session.server, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index the sandbox"))?;
		tangram_server_core::checkpoint!(session.server, "sandbox.control.index.submitted", sandbox = %id).await;

		let forwarded_requests = Arc::new(DashSet::new());
		let capacity = session.server.config.sandbox.control_channel_capacity;
		let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
		let control = tangram_server_core::control::Stream::new(
			stream,
			sender,
			tangram_server_core::control::stream_options(&session.server.config.control),
		);
		let control_sender = control.sender();

		// Watch for the index to record that the connection's boot ended, because the boot's runner may not have learned that its boot was taken over.
		let ended = Arc::new(AtomicBool::new(false));
		let indexed = session
			.server
			.index
			.try_get_sandbox(&id)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox"))?;
		if Self::sandbox_boot_ended(indexed.as_ref(), boot) {
			ended.store(true, Ordering::SeqCst);
		}
		let ended_task = Task::spawn({
			let component = self.clone();
			let ended = ended.clone();
			let id = id.clone();
			let session = session.clone();
			move |_| async move {
				if ended.load(Ordering::SeqCst) {
					return;
				}
				let Ok(mut wakeups) = component
					.create_sandbox_status_wakeup_stream(&session, &id, None, None)
					.await
				else {
					return;
				};
				while wakeups.next().await.is_some() {
					let indexed = session
						.server
						.index
						.try_get_sandbox(&id)
						.await
						.ok()
						.flatten();
					if Self::sandbox_boot_ended(indexed.as_ref(), boot) {
						ended.store(true, Ordering::SeqCst);
						return;
					}
				}
			}
		});

		// Serve the requests to the sandbox's holder by passing them to the connection, and reply that the connection does not hold a boot that the index records as ended.
		let handle = {
			let component = self.clone();
			let ended = ended.clone();
			let forwarded_requests = forwarded_requests.clone();
			let id = id.clone();
			let sender = control_sender.clone();
			let server = session.server.clone();
			move |message: tg::sandbox::control::ServerMessage| {
				let component = component.clone();
				let ended = ended.clone();
				let forwarded_requests = forwarded_requests.clone();
				let id = id.clone();
				let sender = sender.clone();
				let server = server.clone();
				async move {
					if let tg::sandbox::control::ServerMessage::Request(request) = &message
						&& ended.load(Ordering::SeqCst)
					{
						let response = tg::sandbox::control::ClientResponse::with_retry(
							request.id.clone(),
							tg::control::Retry::NotHolder,
						);
						component
							.publish_sandbox_holder_response(&server, &id, response)
							.await?;
						return Ok(());
					}
					let acknowledged_request = match &message {
						tg::sandbox::control::ServerMessage::Ack(ack) => Some(ack.id.clone()),
						_ => None,
					};
					if let tg::sandbox::control::ServerMessage::Request(request) = &message {
						forwarded_requests.insert(request.id.clone());
					}
					sender.send(message).await?;
					if let Some(id) = acknowledged_request {
						forwarded_requests.remove(&id);
					}
					Ok(())
				}
				.boxed()
			}
		};
		let server_messages_task = self
			.serve_sandbox_holder(&session.server, &id, handle)
			.await?;

		// Publish the sandbox's status only now that the connection serves the sandbox's holder, so that a request sent again on the status change reaches the holder.
		Self::spawn_publish_sandbox_status_task(&session.server, &id);

		let control_task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
			let id = id.clone();
			let forwarded_requests = forwarded_requests.clone();
			let runner = runner.clone();

			move |_| async move {
				let mut control = control;
				while let Some(message) = control.recv_without_ack().await? {
					match message {
						tg::sandbox::control::ClientMessage::Ack(ack) => {
							if forwarded_requests.contains(&ack.id) {
								component
									.publish_sandbox_holder_ack(&session.server, &id, ack)
									.await?;
							}
						},
						tg::sandbox::control::ClientMessage::Notification(notification) => {
							match notification {}
						},
						tg::sandbox::control::ClientMessage::Request(request) => {
							let request_id = request.id;
							control.acknowledge(request_id.clone()).await?;
							let result = match request.arg {
								tg::sandbox::control::ClientRequestArg::Destroy(request) => {
									component
										.destroy_sandbox_control_request(
											&session,
											&id,
											request,
											created_at,
											runner.clone(),
										)
										.await
										.map(tg::sandbox::control::ServerResponseOutput::Destroy)
								},
								tg::sandbox::control::ClientRequestArg::Stop(request) => component
									.stop_sandbox_control_request(
										&session,
										&id,
										request,
										created_at,
										runner.clone(),
									)
									.await
									.map(tg::sandbox::control::ServerResponseOutput::Stop),
							};
							let response =
								Self::sandbox_control_server_response(request_id, result);
							control_sender.send(response).await.map_err(|error| {
								tg::error!(!error, "failed to send the sandbox control response")
							})?;
						},
						tg::sandbox::control::ClientMessage::Response(response) => {
							component
								.publish_sandbox_holder_response(&session.server, &id, response)
								.await?;
						},
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let stream = tokio_stream::wrappers::ReceiverStream::new(receiver)
			.attach(server_messages_task)
			.attach(control_task)
			.attach(ended_task)
			.attach(service)
			.map(Ok)
			.with_stopper(session.context.stopper.clone())
			.boxed();

		Self::publish_sandbox_control_connected(&session.server, &id).await?;

		let header = tg::sandbox::control::Header { id, token };

		Ok((header, stream))
	}

	/// Whether the index records that a boot ended, because the sandbox is stopped at the boot, is destroyed, or has a later boot.
	fn sandbox_boot_ended(indexed: Option<&tangram_index::sandbox::Sandbox>, boot: u64) -> bool {
		indexed
			.and_then(|sandbox| sandbox.data.as_ref())
			.is_some_and(|data| {
				data.data.boot > boot
					|| data.data.status.is_destroyed()
					|| (data.data.boot == boot && data.data.status.is_stopped())
			})
	}

	pub async fn prepare_sandbox_control_index_arg(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		create: tg::process::control::SandboxCreate,
	) -> tg::Result<tangram_index::sandbox::put::Arg> {
		let tg::process::control::SandboxCreate {
			arg: create,
			created_at,
			creator,
			owner,
			runner,
		} = create;
		let account = session
			.server
			.components
			.usage
			.usage_account(session, &owner)
			.await?;
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let data = tg::sandbox::get::Output {
			data: tg::sandbox::Data {
				boot: 0,
				cpu: create.cpu,
				creator,
				error: None,
				expires_at: None,
				hostname: create.hostname,
				id: id.clone(),
				memory: create.memory,
				mounts: create.mounts,
				network: create.network,
				owner,
				status: tg::sandbox::Status::Started,
				stopped_at: None,
				storage: create.storage,
				time_to_live: create.time_to_live,
				time_to_stop: create.time_to_stop,
				usage: None,
			},
			location: Some(location.clone()),
			tokens: tg::authorization::Tokens::default(),
		};
		let arg = tangram_index::sandbox::put::Arg {
			account,
			created_at,
			data: Some(data),
			id: id.clone(),
			location: Some(location),
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			process: None,
			processes: None,
			registration: None,
			runner,
			stopped_boot: None,
			stopped_usage: None,
			touched_at: created_at,
		};
		Ok(arg)
	}

	/// Log that a runner reported no usage for a sandbox when this server tracks usage.
	fn warn_missing_sandbox_usage(session: &Session, data: &tg::sandbox::Data) {
		if session.server.config.usage.enabled && data.usage.is_none() {
			tracing::warn!(sandbox = %data.id, "the runner reported no usage for the sandbox");
		}
	}

	fn sandbox_control_server_response(
		id: String,
		result: tg::Result<tg::sandbox::control::ServerResponseOutput>,
	) -> tg::sandbox::control::ServerMessage {
		let (error, output) = match result {
			Ok(output) => (None, Some(output)),
			Err(error) => (
				Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				}),
				None,
			),
		};
		tg::sandbox::control::ServerMessage::Response(tg::sandbox::control::ServerResponse {
			error,
			id,
			output,
		})
	}

	async fn get_sandbox_control_stream_region(
		&self,
		session: &Session,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
		region: String,
	) -> tg::Result<
		ControlFlow<
			(
				tg::sandbox::control::Header,
				BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
			),
			tg::Error,
		>,
	> {
		let id = arg.id.clone();
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, ?id, "failed to get the region client"),
		)?;
		let arg = tg::sandbox::control::Arg {
			location: Some(
				tg::Location::Local(tg::location::Local {
					region: Some(region.clone()),
				})
				.into(),
			),
			..arg
		};
		let output = client
			.get_sandbox_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the control stream"),
			)?
			.map_break(|(header, stream)| {
				let stream = stream.with_stopper(session.context.stopper.clone()).boxed();
				(header, stream)
			})
			.map_continue(
				|error| tg::error!(!error, region = %region, "failed to get the control stream"),
			);
		Ok(output)
	}

	async fn get_sandbox_control_stream_remote(
		&self,
		session: &Session,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::sandbox::control::Header,
				BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
			),
			tg::Error,
		>,
	> {
		let id = arg.id.clone();
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, ?id, "failed to get the remote client"),
			)?;
		let mut context = client.context().clone();
		context.set_token(session.context.token.clone());

		// Remove the claim to act for a process or sandbox, which only a runner's token may make, when the connection uses its own token.
		if session.context.token.is_some() {
			context.set_process(None);
			context.set_sandbox(None);
		}
		let client = client.client().session(&context);
		let arg = tg::sandbox::control::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let output = client
			.get_sandbox_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, "failed to get the control stream"),
			)?
			.map_break(|(header, stream)| {
				let stream = stream.with_stopper(session.context.stopper.clone()).boxed();
				(header, stream)
			})
			.map_continue(
				|error| tg::error!(!error, remote = %remote, "failed to get the control stream"),
			);
		Ok(output)
	}

	pub(crate) async fn send_sandbox_control_request(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		request_id: String,
		arg: tg::sandbox::control::ServerRequestArg,
		options: tangram_server_core::control::Options,
		resend: Option<futures::stream::BoxStream<'static, ()>>,
	) -> tg::Result<
		futures::future::BoxFuture<
			'static,
			tg::Result<tg::Result<tg::sandbox::control::ClientResponseOutput>>,
		>,
	> {
		let kind = match &arg {
			tg::sandbox::control::ServerRequestArg::Destroy(_) => "destroy",
			tg::sandbox::control::ServerRequestArg::Get(_) => "get",
			tg::sandbox::control::ServerRequestArg::GetProcesses(_) => "get_processes",
			tg::sandbox::control::ServerRequestArg::SpawnProcess(_) => "spawn_process",
			tg::sandbox::control::ServerRequestArg::Start(_) => "start",
			tg::sandbox::control::ServerRequestArg::Stop(_) => "stop",
		};
		tangram_server_core::checkpoint!(session.server, "sandbox.control.request", sandbox = %id, kind).await;
		let request = ServerMessage(tg::sandbox::control::ServerMessage::Request(
			tg::sandbox::control::ServerRequest {
				arg,
				id: request_id.clone(),
			},
		));
		let arg = tangram_server_core::control::SendControlRequestArg {
			ack: |id| {
				ServerMessage(tg::sandbox::control::ServerMessage::Ack(
					tg::sandbox::control::ServerAck { id },
				))
			},
			client_subject: format!("sandboxes.{id}.control.client.{request_id}"),
			is_ack: |message: &ClientMessage| {
				matches!(&message.0, tg::sandbox::control::ClientMessage::Ack(_))
			},
			marker: std::marker::PhantomData,
			options,
			request,
			resend,
			response: |message: ClientMessage| {
				let ClientMessage(tg::sandbox::control::ClientMessage::Response(message)) = message
				else {
					return Ok(None);
				};
				let id = message.id.clone();
				let response = match message.into_result() {
					Ok(std::ops::ControlFlow::Break(output)) => {
						std::ops::ControlFlow::Break(Ok(output))
					},
					Ok(std::ops::ControlFlow::Continue(retry)) => {
						std::ops::ControlFlow::Continue(retry)
					},
					Err(error) => std::ops::ControlFlow::Break(Err(error)),
				};
				Ok(Some((id, response)))
			},
			server_subject: format!("sandboxes.{id}.control.server"),
		};
		session.server.send_control_request(arg).await
	}

	pub(crate) async fn get_sandbox_control_stream_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept and content type headers.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()
			.map_err(|error| {
				tg::error!(argument, !error, "failed to parse the content type header")
			})?;
		let tangram_content_type = tg::sandbox::control::TANGRAM_CONTENT_TYPE;
		let output_encoding =
			tangram_client::frame::Encoding::from_accept(accept.as_ref(), tangram_content_type)
				.map_err(|error| tg::error!(argument, !error, "invalid accept type"))?;
		let input_encoding = tangram_client::frame::Encoding::from_content_type(
			content_type
				.as_ref()
				.ok_or_else(|| tg::error!(argument, "missing the content type"))?,
			tangram_content_type,
		)
		.map_err(|error| tg::error!(argument, !error, "invalid content type"))?;

		// Parse the arg.
		let (arg, request) = request
			.arg_with_tangram::<tg::sandbox::control::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Decode the request stream.
		let max_frame_size = session.server.config.sync.max_frame_size;
		let stream = input_encoding.decode(request, max_frame_size);

		// Get the response stream, responding that the server is unavailable if the stream could not reach a server.
		let output = self
			.get_sandbox_control_stream_with_context(session, arg, stream)
			.boxed()
			.await?;
		let (header, stream) = match output {
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => {
				return Err(tg::error!(
					unavailable,
					!error,
					"failed to get the control stream"
				));
			},
		};

		// Create the body.
		let content_type = output_encoding.content_type(tangram_content_type);
		let body = output_encoding.encode(stream, max_frame_size);
		let body = tangram_http::body::header::set(body, &header, output_encoding.serialization())
			.map_err(|error| tg::error!(!error, "failed to serialize the header"))?;

		// Create the response.
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
	}
}

impl tangram_messenger::Payload for ClientMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message =
			serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(message))
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let message =
			serde_json::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(message.into())
	}
}

impl tangram_messenger::Payload for ServerMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message =
			serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(message))
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let message =
			serde_json::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(message.into())
	}
}
