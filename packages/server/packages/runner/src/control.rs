use {
	crate::Component,
	dashmap::DashSet,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::{
		collections::HashSet,
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
	tokio::task::JoinSet,
};

#[derive(Clone)]
pub(crate) struct ClientMessage(pub(crate) tg::runner::control::ClientMessage);

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub(crate) struct ServerMessage {
	connection_index: u64,
	message: tg::runner::control::ServerMessage,
	scheduler: tg::scheduler::Id,
}

impl Component {
	pub(crate) async fn get_runner_control_stream_with_context(
		&self,
		session: &Session,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::runner::control::Header,
				BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>,
			),
			tg::Error,
		>,
	> {
		session.verify_request_from_host()?;
		match &session.context.principal {
			tg::Principal::Root => (),
			tg::Principal::Runner(runner) if runner == &arg.id => (),
			tg::Principal::Runner(runner) => {
				return Err(tg::error!(
					runner = %runner,
					id = %arg.id,
					"invalid runner"
				));
			},
			_ => return Err(tg::error!("unauthorized")),
		}
		let location = session.server.location(arg.location.as_ref())?;
		let output = match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				self.get_runner_control_stream_region(session, arg, stream, region)
					.await?
			},
			tg::Location::Local(_) => {
				let output = self
					.get_runner_control_stream_local(session, arg, stream)
					.boxed()
					.await?;
				ControlFlow::Break(output)
			},
			tg::Location::Remote(tg::location::Remote { name, region }) => {
				self.get_runner_control_stream_remote(session, arg, stream, name, region)
					.await?
			},
		};
		Ok(output)
	}

	async fn get_runner_control_stream_local(
		&self,
		session: &Session,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> tg::Result<(
		tg::runner::control::Header,
		BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>,
	)> {
		let id = arg.id.clone();
		let scheduler_ttl = arg.scheduler_ttl;
		let sandboxes = session.server.index.get_runner_sandboxes(&id).await?;
		let server_messages = session
			.server
			.messenger
			.subscribe::<ServerMessage>(format!("runners.{id}.control.server"))
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to subscribe to the runner server message stream"
				)
			})?;

		let request = tangram_server_core::scheduler::RequestArg::AddRunner(
			tangram_server_core::scheduler::AddRunnerRequestArg {
				capacity: arg.heartbeat.capacity,
				host: arg.host,
				runner: id.clone(),
			},
		);
		let (scheduler, output) = session
			.server
			.components
			.scheduler
			.send_scheduler_request(session, request)
			.boxed()
			.await?;
		let output = output?;
		let output = output
			.try_unwrap_add_runner()
			.map_err(|_| tg::error!("expected an add runner response"))?;
		let connection_index = output.connection_index;
		if output.scheduler != scheduler {
			return Err(tg::error!(
				actual = %output.scheduler,
				expected = %scheduler,
				"the scheduler returned an invalid ID"
			));
		}
		let header = tg::runner::control::Header {
			sandboxes,
			scheduler: scheduler.clone(),
		};
		let heartbeat_subject =
			tangram_server_core::scheduler::runner_heartbeat_subject(&scheduler, &id);
		let heartbeat_acks = session
			.server
			.messenger
			.subscribe::<tangram_messenger::payload::Json<
				tangram_server_core::scheduler::HeartbeatAcknowledgement,
			>>(heartbeat_subject)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to subscribe to the scheduler heartbeat acknowledgements"
				)
			})?;

		let forwarded_requests = Arc::new(DashSet::new());
		let capacity = session.server.config.runner.control_channel_capacity;
		let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
		let control = tangram_server_core::control::Stream::new(
			stream,
			sender.clone(),
			tangram_server_core::control::stream_options(&session.server.config.control),
		);
		let control_sender = control.sender();
		let mut server_messages = server_messages;
		let server_messages_task = Task::spawn({
			let forwarded_requests = forwarded_requests.clone();
			let scheduler = scheduler.clone();
			move |_| async move {
				while let Some(message) = server_messages.try_next().await.map_err(|source| {
					tg::error!(!source, "failed to get a runner server message")
				})? {
					if message.payload.scheduler != scheduler
						|| message.payload.connection_index != connection_index
					{
						continue;
					}
					let message = message.payload.message;
					let acknowledged_request = match &message {
						tg::runner::control::ServerMessage::Ack(ack) => Some(ack.id.clone()),
						_ => None,
					};
					if let tg::runner::control::ServerMessage::Request(request) = &message {
						forwarded_requests.insert(request.id.clone());
					}
					control_sender.send(message).await?;
					if let Some(id) = acknowledged_request {
						forwarded_requests.remove(&id);
					}
				}
				Ok::<_, tg::Error>(())
			}
		});
		let (heartbeat_sender, mut heartbeat_receiver) = tokio::sync::mpsc::channel::<
			tangram_server_core::scheduler::HeartbeatNotification,
		>(capacity);
		let scheduler_unavailable = Arc::new(AtomicBool::new(false));
		let (scheduler_unavailable_sender, scheduler_unavailable_receiver) =
			tokio::sync::oneshot::channel();
		let scheduler_acknowledgement_task = Task::spawn({
			let server = session.server.clone();
			let scheduler = scheduler.clone();
			let scheduler_unavailable = scheduler_unavailable.clone();
			let timeout = scheduler_ttl;
			move |_| async move {
				let mut heartbeat_acks = std::pin::pin!(heartbeat_acks);
				let mut heartbeat_acks_ended = false;
				let mut outstanding = HashSet::new();
				let mut unacknowledged_at = None;
				loop {
					let deadline = unacknowledged_at.map(|time| time + timeout);
					tokio::select! {
						heartbeat = heartbeat_receiver.recv() => {
							let Some(heartbeat) = heartbeat else {
								break;
							};
							tracing::debug!(connection_index, index = heartbeat.heartbeat_index, "forwarding the runner heartbeat");
							outstanding.insert(heartbeat.heartbeat_index);
							unacknowledged_at.get_or_insert_with(|| server.clock.instant());
							if heartbeat_acks_ended {
								break;
							}
							let notification = tangram_server_core::scheduler::Message::Notification(
								tangram_server_core::scheduler::Notification::Heartbeat(heartbeat),
							);
							server
								.messenger
								.publish(
									tangram_server_core::scheduler::scheduler_server_subject(Some(&scheduler)),
									notification,
								)
								.await
								.inspect_err(|error| {
									tracing::error!(
										%error,
										"failed to publish the runner heartbeat notification"
									);
								})
								.ok();
						},
						result = heartbeat_acks.try_next(), if !heartbeat_acks_ended => {
							match result {
								Ok(Some(message)) => {
									let acknowledgement = message.payload.0;
									tracing::debug!(connection_index, index = acknowledgement.heartbeat_index, "received the scheduler heartbeat acknowledgement");
									if acknowledgement.connection_index == connection_index
										&& outstanding.contains(&acknowledgement.heartbeat_index)
									{
										outstanding.clear();
										unacknowledged_at = None;
									}
								},
								Ok(None) => {
									heartbeat_acks_ended = true;
								},
								Err(error) => {
									tracing::error!(
										%error,
										"failed to receive a scheduler heartbeat acknowledgement"
									);
									heartbeat_acks_ended = true;
								},
							}
						},
						() = async {
							match deadline {
								Some(deadline) => server.clock.sleep_until(deadline).await,
								None => future::pending().await,
							}
						} => break,
					}
				}
				if heartbeat_receiver.is_closed() {
					return;
				}
				tracing::warn!(
					connection_index,
					outstanding = outstanding.len(),
					"the scheduler heartbeat acknowledgement expired"
				);
				scheduler_unavailable.store(true, Ordering::Release);
				scheduler_unavailable_sender.send(()).ok();
			}
		});

		let control_task = Task::spawn({
			let component = self.clone();
			let forwarded_requests = forwarded_requests.clone();
			let session = session.clone();
			let runner = id.clone();
			let scheduler = scheduler.clone();
			let scheduler_unavailable = scheduler_unavailable.clone();
			move |_| async move {
				let mut control = control;
				let mut requests = JoinSet::<tg::Result<()>>::new();
				loop {
					let message = tokio::select! {
						message = control.recv_without_ack() => message?,
						result = requests.join_next(), if !requests.is_empty() => {
							result.unwrap().map_err(|error| tg::error!(!error, "the runner control request task panicked"))??;
							continue;
						},
					};
					let Some(message) = message else { break };
					match message {
						tg::runner::control::ClientMessage::Ack(ack) => {
							if forwarded_requests.contains(&ack.id) {
								component
									.publish_runner_control_ack(&session, &runner, ack)
									.await?;
							}
						},
						tg::runner::control::ClientMessage::Notification(
							tg::runner::control::ClientNotification::Heartbeat(heartbeat),
						) => {
							tracing::debug!(%runner, connection_index, index = heartbeat.index, "received the runner heartbeat");
							tangram_server_core::checkpoint!(session.server, "runner.control.heartbeat.received", %runner).await;
							let notification =
								tangram_server_core::scheduler::HeartbeatNotification {
									capacity: heartbeat.capacity,
									connection_index,
									heartbeat_index: heartbeat.index,
									runner: runner.clone(),
									sandboxes: heartbeat.sandboxes,
								};
							heartbeat_sender.send(notification).await.map_err(|_| {
								tg::error!("failed to track the runner heartbeat acknowledgement")
							})?;
						},
						tg::runner::control::ClientMessage::Request(request) => {
							control.acknowledge(request.id.clone()).await?;
							let sender = control.sender();
							let component = component.clone();
							let session = session.clone();
							let runner = runner.clone();
							requests.spawn(async move {
								let response = component
									.handle_runner_control_request(&session, &runner, request)
									.await;
								sender.send(response).await?;
								Ok(())
							});
						},
						tg::runner::control::ClientMessage::Response(response) => {
							component
								.publish_runner_control_response(&session, &runner, response)
								.await?;
						},
					}
				}
				if !scheduler_unavailable.load(Ordering::Acquire) {
					let request = tangram_server_core::scheduler::RequestArg::RemoveRunner(
						tangram_server_core::scheduler::RemoveRunnerRequestArg {
							connection_index,
							runner: runner.clone(),
						},
					);
					session
						.server
						.components
						.scheduler
						.send_scheduler_request_to(&session, &scheduler, request)
						.boxed()
						.await??;
				}
				Ok::<_, tg::Error>(())
			}
		});

		let stream = tokio_stream::wrappers::ReceiverStream::new(receiver)
			.take_until(async move {
				scheduler_unavailable_receiver.await.ok();
			})
			.attach(server_messages_task)
			.attach(scheduler_acknowledgement_task)
			.attach(control_task)
			.map(Ok)
			.with_stopper(session.context.stopper.clone())
			.boxed();

		Ok((header, stream))
	}

	async fn handle_runner_control_request(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		request: tg::runner::control::ClientRequest,
	) -> tg::runner::control::ServerMessage {
		let result = match request.arg {
			tg::runner::control::ClientRequestArg::DestroySandbox(arg) => self
				.destroy_runner_sandbox(session, runner, &arg.sandbox)
				.await
				.map(tg::runner::control::ServerResponseOutput::DestroySandbox),
		};
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
		let response = tg::runner::control::ServerResponse {
			error,
			id: request.id,
			output,
		};
		tg::runner::control::ServerMessage::Response(response)
	}

	async fn destroy_runner_sandbox(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<tg::runner::control::DestroySandboxServerResponseOutput> {
		let indexed = session.server.index.try_get_sandbox(sandbox).await?;
		let destroyed = if indexed.as_ref().is_some_and(|indexed| {
			indexed.runner.as_ref() == Some(runner)
				&& indexed
					.data
					.as_ref()
					.is_some_and(|data| data.data.status.is_started())
		}) {
			tangram_server_core::checkpoint!(session.server, "runner.control.destroy_sandbox", %runner, %sandbox)
				.await;
			session
				.server
				.components
				.scheduler
				.destroy_runner_sandbox(&session.server, sandbox, "runner restarted")
				.await?;
			true
		} else {
			false
		};
		let output = tg::runner::control::DestroySandboxServerResponseOutput { destroyed };
		Ok(output)
	}

	async fn publish_runner_control_ack(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		ack: tg::runner::control::ClientAck,
	) -> tg::Result<()> {
		let subject = format!("runners.{runner}.control.client.{}", ack.id);
		let payload = ClientMessage(tg::runner::control::ClientMessage::Ack(ack));
		session
			.server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to publish the runner control acknowledgement"
				)
			})?;

		Ok(())
	}

	async fn publish_runner_control_response(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		response: tg::runner::control::ClientResponse,
	) -> tg::Result<()> {
		let subject = format!("runners.{runner}.control.client.{}", response.id);
		let payload = ClientMessage(tg::runner::control::ClientMessage::Response(response));
		session
			.server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the runner client message"))?;

		Ok(())
	}

	async fn get_runner_control_stream_region(
		&self,
		session: &Session,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
		region: String,
	) -> tg::Result<
		ControlFlow<
			(
				tg::runner::control::Header,
				BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>,
			),
			tg::Error,
		>,
	> {
		let id = arg.id.clone();
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::runner::control::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client
			.get_runner_control_stream(arg, stream)
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

	async fn get_runner_control_stream_remote(
		&self,
		session: &Session,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::runner::control::Header,
				BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>,
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
				|error| tg::error!(!error, remote = %remote, %id, "failed to get the remote client"),
			)?;
		let trusted = client.trusted();
		let arg = tg::runner::control::Arg {
			location: Some(
				tg::Location::Local(tg::location::Local {
					region: region.clone(),
				})
				.into(),
			),
			..arg
		};
		let output = client
			.get_runner_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, "failed to get the control stream"),
			)?;
		let (header, stream) = match output {
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => {
				let error =
					tg::error!(!error, remote = %remote, "failed to get the control stream");
				return Ok(ControlFlow::Continue(error));
			},
		};
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote,
			region,
		});
		let stream = stream
			.map({
				let session = session.clone();
				move |result| {
					let mut message = result?;
					if let tg::runner::control::ServerMessage::Request(request) = &mut message
						&& let tg::runner::control::ServerRequestArg::CreateSandbox(arg) =
							&mut request.arg && let Some(process) = &mut arg.process
					{
						session
							.server
							.components
							.authorization
							.update_process_data_referents_for_location(
								&session,
								&mut process.data,
								&location,
								trusted,
							)?;
						session
							.server
							.components
							.authorization
							.update_tokens_and_location(
								&session,
								&mut process.options.tokens,
								Some(&mut process.options.location),
								&location,
								trusted,
							)?;
					}
					Ok(message)
				}
			})
			.with_stopper(session.context.stopper.clone())
			.boxed();
		Ok(ControlFlow::Break((header, stream)))
	}

	pub(crate) async fn get_runner_control_stream_request(
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
		let tangram_content_type = tg::runner::control::TANGRAM_CONTENT_TYPE;
		let output_encoding = tangram_server_core::http::frame::Encoding::from_accept(
			accept.as_ref(),
			tangram_content_type,
		)
		.map_err(|error| tg::error!(argument, !error, "invalid accept type"))?;
		let input_encoding = tangram_server_core::http::frame::Encoding::from_content_type(
			content_type
				.as_ref()
				.ok_or_else(|| tg::error!(argument, "missing the content type"))?,
			tangram_content_type,
		)
		.map_err(|error| tg::error!(argument, !error, "invalid content type"))?;

		// Parse the arg.
		let (arg, request) = request
			.arg::<tg::runner::control::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.ok_or_else(|| tg::error!(argument, "missing the query params"))?;

		// Decode the request stream.
		let max_frame_size = session.server.config.sync.max_frame_size;
		let stream =
			tangram_server_core::http::frame::decode(request, input_encoding, max_frame_size);
		let stream = stream.with_stopper(session.context.stopper.clone()).boxed();

		// Get the response stream, responding that the server is unavailable if the stream could not reach a server.
		let output = self
			.get_runner_control_stream_with_context(session, arg, stream)
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
		let body =
			tangram_server_core::http::frame::encode(stream, output_encoding, max_frame_size);
		let body = tangram_http::body::header::set(body, &header, output_encoding.serialization())
			.map_err(|error| tg::error!(!error, "failed to serialize the header"))?;

		// Create the response.
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
	}

	pub async fn send_runner_control_request(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		scheduler: &tg::scheduler::Id,
		connection_index: u64,
		arg: tg::runner::control::ServerRequestArg,
		options: tangram_server_core::control::Options,
	) -> tg::Result<tg::Result<tg::runner::control::ClientResponseOutput>> {
		let id = tangram_server_core::control::id();
		let request =
			tg::runner::control::ServerMessage::Request(tg::runner::control::ServerRequest {
				arg,
				id: id.clone(),
			});
		let request = ServerMessage {
			connection_index,
			message: request,
			scheduler: scheduler.clone(),
		};
		let scheduler = scheduler.clone();
		session
			.server
			.request_control(tangram_server_core::control::SendControlRequestArg {
				ack: move |id| {
					let message =
						tg::runner::control::ServerMessage::Ack(tg::runner::control::ServerAck {
							id,
						});
					ServerMessage {
						connection_index,
						message,
						scheduler: scheduler.clone(),
					}
				},
				client_subject: format!("runners.{runner}.control.client.{id}"),
				is_ack: |message: &ClientMessage| {
					matches!(&message.0, tg::runner::control::ClientMessage::Ack(_))
				},
				marker: std::marker::PhantomData,
				options,
				request,
				response: |message: ClientMessage| {
					let tg::runner::control::ClientMessage::Response(message) = message.0 else {
						return Ok(None);
					};
					if let Some(error) = message.error {
						let error = tg::Error::try_from(error).map_err(|source| {
							tg::error!(!source, "failed to deserialize the error")
						})?;
						return Ok(Some((message.id, Err(error))));
					}
					let Some(output) = message.output else {
						return Err(tg::error!("missing runner control response output"));
					};
					Ok(Some((message.id, Ok(output))))
				},
				server_subject: format!("runners.{runner}.control.server"),
			})
			.boxed()
			.await
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
		Ok(message)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let message = serde_json::to_vec(self).map_err(tangram_messenger::Error::serialization)?;
		Ok(message.into())
	}
}
