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
	tangram_futures::{
		stream::{Ext as _, TryExt as _},
		task::Task,
	},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
	tangram_messenger::Messenger as _,
	tangram_server_core::Session,
	tokio::task::JoinSet,
};

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub(crate) struct ClientMessage(pub(crate) tg::runner::control::ClientMessage);

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub(crate) struct ServerMessage {
	connection: u64,
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
		let scheduler_time_to_live = arg.scheduler_time_to_live;

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

		tangram_server_core::checkpoint!(session.server, "runner.connection.start", runner = %id)
			.await;
		let request = tangram_server_scheduler_core::RequestArg::AddRunner(
			tangram_server_scheduler_core::AddRunnerRequestArg {
				capacity: arg.heartbeat.capacity,
				first: arg.first,
				host: arg.host,
				runner: id.clone(),
				sandboxes: arg.sandboxes,
			},
		);

		// Connect to the scheduler that the runner belongs to, or to any scheduler on the runner's first connection, which binds the runner to that scheduler for its whole life.
		let owner = session
			.server
			.index
			.try_get_runner(&id)
			.await
			.map_err(|error| tg::error!(!error, runner = %id, "failed to get the runner"))?
			.and_then(|runner| runner.scheduler);
		let (scheduler, output) = if let Some(scheduler) = owner {
			let output = session
				.server
				.components
				.scheduler
				.send_scheduler_request_to(session, &scheduler, request)
				.boxed()
				.await?;
			(scheduler, output)
		} else {
			session
				.server
				.components
				.scheduler
				.send_scheduler_request(session, request)
				.boxed()
				.await?
		};
		let output = output?;
		let output = output
			.try_unwrap_add_runner()
			.map_err(|_| tg::error!("expected an add runner response"))?;
		if output.scheduler != scheduler {
			return Err(tg::error!(
				actual = %output.scheduler,
				expected = %scheduler,
				"the scheduler returned an invalid ID"
			));
		}
		let connection = output.connection;

		tangram_server_core::checkpoint!(session.server, "runner.connection.accept", runner = %id, connection).await;

		let runner_time_to_live = session.server.config.scheduler.runner_time_to_live;
		let header = tg::runner::control::Header {
			ended: output.ended,
			runner_time_to_live,
			scheduler: scheduler.clone(),
		};
		let heartbeat_subject =
			tangram_server_scheduler_core::runner_heartbeat_subject(&scheduler, &id);
		let heartbeat_acks = session
			.server
			.messenger
			.subscribe::<tangram_server_scheduler_core::HeartbeatAcknowledgement>(heartbeat_subject)
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
						|| message.payload.connection != connection
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
			tangram_server_scheduler_core::HeartbeatNotification,
		>(capacity);
		let scheduler_unavailable = Arc::new(AtomicBool::new(false));
		let (scheduler_unavailable_sender, scheduler_unavailable_receiver) =
			tokio::sync::oneshot::channel();
		let scheduler_acknowledgement_task = Task::spawn({
			let server = session.server.clone();
			let scheduler = scheduler.clone();
			let scheduler_unavailable = scheduler_unavailable.clone();
			let timeout = scheduler_time_to_live;
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
							tracing::debug!(connection, index = heartbeat.heartbeat_index, "forwarding the runner heartbeat");
							outstanding.insert(heartbeat.heartbeat_index);
							unacknowledged_at.get_or_insert_with(|| server.clock.instant());
							if heartbeat_acks_ended {
								break;
							}
							let notification =
								tangram_server_scheduler_core::Notification::Heartbeat(heartbeat);
							server
								.messenger
								.publish(
									tangram_server_scheduler_core::scheduler_notification_subject(&scheduler),
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
									let acknowledgement = message.payload;
									tracing::debug!(connection, index = acknowledgement.heartbeat_index, "received the scheduler heartbeat acknowledgement");
									if acknowledgement.connection == connection
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
					connection,
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
				let mut ended = false;
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
							tg::runner::control::ClientNotification::End,
						) => {
							ended = true;
							break;
						},
						tg::runner::control::ClientMessage::Notification(
							tg::runner::control::ClientNotification::Heartbeat(heartbeat),
						) => {
							tracing::debug!(%runner, connection, index = heartbeat.index, "received the runner heartbeat");
							tangram_server_core::checkpoint!(session.server, "runner.control.heartbeat.received", %runner).await;
							let notification =
								tangram_server_scheduler_core::HeartbeatNotification {
									capacity: heartbeat.capacity,
									connection,
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
							let scheduler = scheduler.clone();
							requests.spawn(async move {
								let response = component
									.handle_runner_control_request(
										&session, &runner, &scheduler, connection, request,
									)
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
				// Remove the runner at once only if it ended the stream because it shut down, and leave a stream that ended otherwise to the runner's ttl and its next connection.
				if ended && !scheduler_unavailable.load(Ordering::Acquire) {
					let request = tangram_server_scheduler_core::RequestArg::RemoveRunner(
						tangram_server_scheduler_core::RemoveRunnerRequestArg {
							connection,
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
				tangram_server_core::checkpoint!(session.server, "runner.control.ended", %runner)
					.await;
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
		scheduler: &tg::scheduler::Id,
		connection: u64,
		request: tg::runner::control::ClientRequest,
	) -> tg::runner::control::ServerMessage {
		let result = match request.arg {
			tg::runner::control::ClientRequestArg::BootEnded(arg) => self
				.runner_boot_ended(session, runner, scheduler, arg)
				.await
				.map(|(output, retry)| {
					let output = tg::runner::control::ServerResponseOutput::BootEnded(output);
					(output, retry)
				}),
			tg::runner::control::ClientRequestArg::DestroySandbox(arg) => self
				.destroy_runner_sandbox(session, runner, scheduler, arg)
				.await
				.map(|(output, retry)| {
					let output = tg::runner::control::ServerResponseOutput::DestroySandbox(output);
					(output, retry)
				}),
			tg::runner::control::ClientRequestArg::DestroyStoppedSandbox(arg) => self
				.destroy_runner_stopped_sandbox(session, runner, arg)
				.await
				.map(|output| {
					let output =
						tg::runner::control::ServerResponseOutput::DestroyStoppedSandbox(output);
					(output, None)
				}),
			tg::runner::control::ClientRequestArg::FinishProcesses(arg) => self
				.finish_runner_processes(session, runner, scheduler, arg)
				.await
				.map(|(output, retry)| {
					let output = tg::runner::control::ServerResponseOutput::FinishProcesses(output);
					(output, retry)
				}),
			tg::runner::control::ClientRequestArg::ReportSandboxUsage(arg) => self
				.report_runner_sandbox_usage(session, runner, arg)
				.await
				.map(|output| {
					let output =
						tg::runner::control::ServerResponseOutput::ReportSandboxUsage(output);
					(output, None)
				}),
			tg::runner::control::ClientRequestArg::SandboxCreated(arg) => self
				.runner_sandbox_created(session, runner, scheduler, connection, arg)
				.await
				.map(|(output, retry)| {
					let output = tg::runner::control::ServerResponseOutput::SandboxCreated(output);
					(output, retry)
				}),
		};
		let (error, output, retry) = match result {
			Ok((output, retry)) => (None, Some(output), retry),
			Err(error) => (
				Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				}),
				None,
				None,
			),
		};
		let response = tg::runner::control::ServerResponse {
			error,
			id: request.id,
			output,
			retry,
		};
		tg::runner::control::ServerMessage::Response(response)
	}

	/// Report a sandbox that a runner started on the shortcut path to the runner's scheduler, and ask the runner to send the report again if the scheduler cannot be reached or asks for the report again.
	async fn runner_sandbox_created(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		scheduler: &tg::scheduler::Id,
		connection: u64,
		arg: tg::runner::control::SandboxCreatedClientRequestArg,
	) -> tg::Result<(
		tg::runner::control::SandboxCreatedServerResponseOutput,
		Option<tg::control::Retry>,
	)> {
		let request = tangram_server_scheduler_core::RequestArg::RunnerSandboxCreated(
			tangram_server_scheduler_core::RunnerSandboxCreatedRequestArg {
				boot: arg.boot,
				connection,
				proof: arg.proof,
				runner: runner.clone(),
				sandbox: arg.sandbox,
			},
		);
		let output = tg::runner::control::SandboxCreatedServerResponseOutput {};
		let response = match session
			.server
			.components
			.scheduler
			.send_scheduler_request_to(session, scheduler, request)
			.boxed()
			.await
		{
			Ok(response) => response?,
			Err(error) => {
				tracing::warn!(error = %error.trace(), "failed to send the report to the scheduler");
				return Ok((output, Some(tg::control::Retry::NotReady)));
			},
		}
		.try_unwrap_runner_sandbox_created()
		.map_err(|_| tg::error!("expected a runner sandbox created response"))?;

		Ok((output, response.retry))
	}

	/// Send a runner's report that it removed the directory of a destroying sandbox to the runner's scheduler, which authorizes it from its memory of the removal that it asked of the runner and records the destruction, and ask the runner to send the report again if the scheduler cannot be reached or cannot authorize it yet.
	async fn destroy_runner_sandbox(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		scheduler: &tg::scheduler::Id,
		arg: tg::runner::control::DestroySandboxClientRequestArg,
	) -> tg::Result<(
		tg::runner::control::DestroySandboxServerResponseOutput,
		Option<tg::control::Retry>,
	)> {
		let error = match arg.error {
			Some(tg::Either::Left(data)) => Some(data),
			Some(tg::Either::Right(id)) => Some(
				tg::Error::with_id(id)
					.data_with_instance(session)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the sandbox error"))?,
			),
			None => None,
		};
		let request = tangram_server_scheduler_core::RequestArg::RecordRunnerSandboxDestruction(
			tangram_server_scheduler_core::RecordRunnerSandboxDestructionRequestArg {
				error,
				runner: runner.clone(),
				sandbox: arg.sandbox,
			},
		);
		let output = match session
			.server
			.components
			.scheduler
			.send_scheduler_request_to(session, scheduler, request)
			.boxed()
			.await
		{
			Ok(output) => output?,
			Err(error) => {
				tracing::warn!(error = %error.trace(), "failed to send the report to the scheduler");
				let output =
					tg::runner::control::DestroySandboxServerResponseOutput { destroyed: false };
				return Ok((output, Some(tg::control::Retry::NotReady)));
			},
		}
		.try_unwrap_record_runner_sandbox_destruction()
		.map_err(|_| tg::error!("expected a record runner sandbox destruction response"))?;
		let retry = output.retry;
		let output = tg::runner::control::DestroySandboxServerResponseOutput {
			destroyed: output.destroyed,
		};

		Ok((output, retry))
	}

	/// Ask that a sandbox that is stopped at a boot on the runner be destroyed, which the index decides with the write that records the sandbox as destroying only while it is still stopped at the boot. The sandbox's scheduler then has the runner remove the directory and records the destruction.
	async fn destroy_runner_stopped_sandbox(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		arg: tg::runner::control::DestroyStoppedSandboxClientRequestArg,
	) -> tg::Result<tg::runner::control::DestroyStoppedSandboxServerResponseOutput> {
		let destroying = session
			.server
			.components
			.sandbox
			.destroy_stopped_sandbox_at_boot(session, &arg.sandbox, runner, arg.boot)
			.await?;
		let output = tg::runner::control::DestroyStoppedSandboxServerResponseOutput { destroying };

		Ok(output)
	}

	/// Send a runner's report that a boot that it held ended to the runner's scheduler, which authorizes it from its memory of the boot and records it, and ask the runner to send the report again if the scheduler cannot be reached or cannot authorize or record it yet.
	async fn runner_boot_ended(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		scheduler: &tg::scheduler::Id,
		arg: tg::runner::control::BootEndedClientRequestArg,
	) -> tg::Result<(
		tg::runner::control::BootEndedServerResponseOutput,
		Option<tg::control::Retry>,
	)> {
		let request = tangram_server_scheduler_core::RequestArg::RunnerBootEnded(
			tangram_server_scheduler_core::RunnerBootEndedRequestArg {
				end: arg,
				runner: runner.clone(),
			},
		);
		let output = match session
			.server
			.components
			.scheduler
			.send_scheduler_request_to(session, scheduler, request)
			.boxed()
			.await
		{
			Ok(output) => output?,
			Err(error) => {
				tracing::warn!(error = %error.trace(), "failed to send the report to the scheduler");
				let output = tg::runner::control::BootEndedServerResponseOutput {};
				return Ok((output, Some(tg::control::Retry::NotReady)));
			},
		}
		.try_unwrap_runner_boot_ended()
		.map_err(|_| tg::error!("expected a runner boot ended response"))?;
		let retry = output.retry;
		let output = tg::runner::control::BootEndedServerResponseOutput {};

		Ok((output, retry))
	}

	/// Send a runner's report of processes that it accepted and that cannot run, such as a process for a boot that ended, to the runner's scheduler, which authorizes it from its memory of the boot and of the processes that it offered into the boot and finishes them, and ask the runner to send the report again if the scheduler cannot be reached or cannot authorize or finish them yet.
	async fn finish_runner_processes(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		scheduler: &tg::scheduler::Id,
		arg: tg::runner::control::FinishProcessesClientRequestArg,
	) -> tg::Result<(
		tg::runner::control::FinishProcessesServerResponseOutput,
		Option<tg::control::Retry>,
	)> {
		let request = tangram_server_scheduler_core::RequestArg::FinishRunnerProcesses(
			tangram_server_scheduler_core::FinishRunnerProcessesRequestArg {
				finish: arg,
				runner: runner.clone(),
			},
		);
		let output = match session
			.server
			.components
			.scheduler
			.send_scheduler_request_to(session, scheduler, request)
			.boxed()
			.await
		{
			Ok(output) => output?,
			Err(error) => {
				tracing::warn!(error = %error.trace(), "failed to send the report to the scheduler");
				let output = tg::runner::control::FinishProcessesServerResponseOutput {};
				return Ok((output, Some(tg::control::Retry::NotReady)));
			},
		}
		.try_unwrap_finish_runner_processes()
		.map_err(|_| tg::error!("expected a finish runner processes response"))?;
		let retry = output.retry;
		let output = tg::runner::control::FinishProcessesServerResponseOutput {};

		Ok((output, retry))
	}

	async fn report_runner_sandbox_usage(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		arg: tg::runner::control::ReportSandboxUsageClientRequestArg,
	) -> tg::Result<tg::runner::control::ReportSandboxUsageServerResponseOutput> {
		// Read the reported sandboxes from the index in one batch.
		let ids = arg
			.sandboxes
			.iter()
			.map(|sandbox| sandbox.sandbox.clone())
			.collect::<Vec<_>>();
		let mut indexed = session.server.index.try_get_sandboxes(&ids).await?;

		// Await indexing and read again the reported sandboxes that the index does not record, because the index lags, so that a directory is removed as unrecorded only once the index has every write that was acknowledged before.
		let unrecorded = std::iter::zip(&ids, &indexed)
			.filter(|(_, indexed)| indexed.is_none())
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		if !unrecorded.is_empty() {
			session
				.server
				.components
				.indexer
				.index(session, tg::index::Arg::default())
				.await
				.map_err(|error| tg::error!(!error, "failed to index"))?
				.try_last()
				.await
				.map_err(|error| tg::error!(!error, "failed to index"))?;
			let mut read = session
				.server
				.index
				.try_get_sandboxes(&unrecorded)
				.await?
				.into_iter();
			for indexed in indexed.iter_mut().filter(|indexed| indexed.is_none()) {
				*indexed = read.next().flatten();
			}
		}

		// Decide what the runner does with each sandbox, and accept the usage of a sandbox that this runner holds, that is not destroyed, and that is at the boot or a later one, because a restart reports the previous boot's usage while its start is recorded. A destroying sandbox's directory occupies storage until the runner removes it.
		let touched_at = session.server.clock.unix_timestamp();
		let mut items = Vec::new();
		let mut sandboxes = Vec::with_capacity(arg.sandboxes.len());
		for (reported, indexed) in std::iter::zip(arg.sandboxes, indexed) {
			let tg::runner::control::ReportSandboxUsageClientRequestSandbox {
				boot,
				sandbox,
				usage,
			} = reported;
			let action = stopped_sandbox_action(indexed.as_ref(), runner);
			let accepted = indexed.filter(|indexed| {
				indexed.runner.as_ref() == Some(runner)
					&& indexed.data.as_ref().is_some_and(|data| {
						!data.data.status.is_destroyed() && data.data.boot >= boot
					})
			});
			if let (Some(indexed), Some(usage)) = (accepted, usage) {
				let stopped_usage = tangram_index::sandbox::put::StoppedUsage { boot, usage };
				let put_sandbox = tangram_index::sandbox::put::Arg {
					account: indexed.account,
					created_at: indexed.created_at,
					data: None,
					id: sandbox.clone(),
					location: None,
					permissions: Vec::new(),
					principal: tg::Principal::Sandbox(sandbox.clone()),
					process: None,
					processes: None,
					runner: None,
					scheduler: None,
					sequence: 0,
					stopped_boot: None,
					stopped_usage: Some(stopped_usage),
					touched_at,
				};
				items.push(tangram_index::Item::PutSandbox(Box::new(put_sandbox)));
			}
			let entry =
				tg::runner::control::ReportSandboxUsageServerResponseSandbox { action, sandbox };
			sandboxes.push(entry);
		}

		// Record the stopped boots' storage usage directly in the index in one write, which merges it with the earlier reports, so that it is recorded before the runner reports a sandbox's destruction, which bills it.
		let arg = tangram_index::write::Arg { items };
		let result =
			session.server.index.write(arg).await.map_err(|error| {
				tg::error!(!error, "failed to index the stopped sandboxes' usage")
			})?;
		if let Err(error) = result {
			tracing::warn!(error = %error.trace(), "the index rejected a stopped sandbox's usage");
		}
		let output = tg::runner::control::ReportSandboxUsageServerResponseOutput { sandboxes };

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
						&& let tg::runner::control::ServerRequestArg::OfferBoot(arg) =
							&mut request.arg
						&& let Some(process) = &mut arg.process
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
			.arg_with_tangram::<tg::runner::control::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.ok_or_else(|| tg::error!(argument, "missing the query params"))?;

		// Decode the request stream.
		let max_frame_size = session.server.config.sync.max_frame_size;
		let stream = input_encoding.decode(request, max_frame_size);
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

	#[expect(clippy::too_many_arguments)]
	pub async fn send_runner_control_request(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		scheduler: &tg::scheduler::Id,
		connection: u64,
		id: String,
		arg: tg::runner::control::ServerRequestArg,
		options: tangram_server_core::control::Options,
	) -> tg::Result<tg::Result<tg::runner::control::ClientResponseOutput>> {
		let request =
			tg::runner::control::ServerMessage::Request(tg::runner::control::ServerRequest {
				arg,
				id: id.clone(),
			});
		let request = ServerMessage {
			connection,
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
						connection,
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
				resend: None,
				response: |message: ClientMessage| {
					let tg::runner::control::ClientMessage::Response(message) = message.0 else {
						return Ok(None);
					};
					if let Some(error) = message.error {
						let error = tg::Error::try_from(error).map_err(|source| {
							tg::error!(!source, "failed to deserialize the error")
						})?;
						return Ok(Some((message.id, ControlFlow::Break(Err(error)))));
					}
					let Some(output) = message.output else {
						return Err(tg::error!("missing runner control response output"));
					};
					Ok(Some((message.id, ControlFlow::Break(Ok(output)))))
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

/// Decide what a runner does with the directory of a sandbox that it reported and does not hold, from the index's record after indexing was awaited for an unrecorded sandbox. It removes and reports the destruction of a sandbox that the index records on the runner as destroyed or destroying, removes only the directory of a sandbox that the index does not record or records on another runner, and keeps the directory of every other sandbox that the index records on the runner or without a runner, which is stopped, or whose boot is queued for the runner or ends.
fn stopped_sandbox_action(
	indexed: Option<&tangram_index::sandbox::Sandbox>,
	runner: &tg::runner::Id,
) -> tg::runner::control::StoppedSandboxAction {
	let Some(indexed) = indexed.filter(|indexed| {
		indexed
			.runner
			.as_ref()
			.is_none_or(|indexed| indexed == runner)
	}) else {
		return tg::runner::control::StoppedSandboxAction::Remove(
			tg::runner::control::StoppedSandboxRemoval::Unrecorded,
		);
	};
	let status = indexed.data.as_ref().map(|data| data.data.status);
	match status {
		Some(tg::sandbox::Status::Destroyed) => tg::runner::control::StoppedSandboxAction::Remove(
			tg::runner::control::StoppedSandboxRemoval::Destroyed,
		),
		Some(tg::sandbox::Status::Destroying) => tg::runner::control::StoppedSandboxAction::Remove(
			tg::runner::control::StoppedSandboxRemoval::Destroying,
		),
		_ => tg::runner::control::StoppedSandboxAction::Keep,
	}
}
