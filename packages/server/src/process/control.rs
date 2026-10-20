use {
	crate::Session,
	dashmap::DashSet,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream::BoxStream},
	num::ToPrimitive as _,
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::{
		stream::{Ext as _, TryExt as _},
		task::Task,
	},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_index::prelude::*,
	tangram_messenger::Messenger,
};

pub(crate) mod connection;
pub(crate) mod finish;
pub(crate) mod local;
pub(crate) mod service;

#[derive(Clone)]
pub(crate) struct ClientMessage(pub(crate) tg::process::control::ClientMessage);

#[derive(Clone)]
pub(crate) struct ServerMessage(pub(crate) tg::process::control::ServerMessage);

#[derive(Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct Connected {
	#[tangram_serialize(id = 0)]
	pub lease: tg::process::Lease,
}

/// The syncs that push a process's command objects and outcome objects, so that each push is a transfer of its own.
#[derive(Clone)]
struct Syncs {
	command: tg::Referent<tg::sync::Id>,
	outcome: tg::Referent<tg::sync::Id>,
}

struct IndexProcessControlArg {
	assign: bool,
	data: tg::process::Data,
	id: tg::process::Id,
	options: tg::referent::Options,
	parent: Option<tg::process::Id>,
	sandbox: Option<tg::process::control::SandboxCreate>,
}

pub(crate) fn connected_subject(id: &tg::process::Id) -> String {
	format!("processes.{id}.control.connected")
}

impl Session {
	pub(crate) async fn subscribe_process_connection(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<super::ConnectionFuture> {
		let mut event_stream = self
			.server
			.messenger
			.subscribe::<Connected>(connected_subject(id))
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					process = %id,
					"failed to subscribe to the process control connection"
				)
			})?;
		let id = id.clone();
		let future = async move {
			let event = event_stream
				.try_next()
				.await
				.map_err(|error| {
					tg::error!(
						!error,
						process = %id,
						"failed to receive the process control connection"
					)
				})?
				.ok_or_else(|| {
					tg::error!(
						process = %id,
						"the process control connection subscription ended"
					)
				})?;

			Ok(event.payload)
		}
		.boxed();

		Ok(future)
	}

	pub(crate) async fn try_get_process_control_stream_with_context(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<
		Option<(
			tg::process::control::Header,
			BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
		)>,
	> {
		self.try_get_process_control_stream_inner(arg, stream)
			.boxed()
			.await
	}

	pub(crate) async fn try_get_process_control_stream_inner(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<
		Option<(
			tg::process::control::Header,
			BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
		)>,
	> {
		let location = self.server.location(arg.location.as_ref())?;
		let output = match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != self.server.config.region.as_deref() => {
				self.try_get_process_control_stream_region(arg, stream, region)
					.await?
			},
			tg::Location::Local(_) => {
				self.try_get_process_control_stream_local(arg, stream)
					.boxed()
					.await?
			},
			tg::Location::Remote(tg::location::Remote { name, region }) => {
				self.try_get_process_control_stream_remote(arg, stream, name, region)
					.await?
			},
		};
		Ok(output)
	}

	async fn try_get_process_control_stream_local(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<
		Option<(
			tg::process::control::Header,
			BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
		)>,
	> {
		self.try_get_process_control_stream_local_inner(
			arg,
			stream,
			self.context.local_process_control,
		)
		.boxed()
		.await
	}

	async fn try_get_process_control_stream_local_inner(
		&self,
		mut arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
		local_process_control: bool,
	) -> tg::Result<
		Option<(
			tg::process::control::Header,
			BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
		)>,
	> {
		// Authenticate the connection as its process, or as a runner that proves that it derived the process's ID, which starts or resumes a process on the shortcut path.
		let id = arg.id.clone();
		let proof = arg.proof.take();
		let shortcut_start = matches!(
			&arg.mode,
			tg::process::control::Mode::Start(start) if start.shortcut.is_some()
		);
		let (shortcut, runner, token) = match (&self.context.principal, proof) {
			(tg::Principal::Process(process), None) if process == &id => {
				(false, None, self.context.token.clone())
			},
			(tg::Principal::Runner(runner), Some(proof)) => {
				self.verify_process_control_proof(&id, runner, &proof, &arg.mode)
					.await?;
				let token = self
					.server
					.create_process_authentication_token(id.clone())?;
				(true, Some(runner.clone()), Some(token))
			},
			(tg::Principal::Root, None) => {
				let token = self
					.server
					.create_process_authentication_token(id.clone())?;
				(shortcut_start, None, Some(token))
			},
			_ => {
				return Err(tg::error!(unauthorized, %id, "unauthorized"));
			},
		};
		let context = crate::Context {
			principal: tg::Principal::Process(id.clone()),
			token: token.clone(),
			..self.context.clone()
		};
		let session = self.server.session(&context);

		// An in-process runner stores its outcome locally, so it needs no syncs.
		let syncs = if local_process_control {
			None
		} else {
			let command = session.refresh_sync(tg::sync::Id::with_process(
				&id,
				tg::sync::join::Kind::Command,
			))?;
			let outcome = session.refresh_sync(tg::sync::Id::with_process(
				&id,
				tg::sync::join::Kind::Outcome,
			))?;
			Some(Syncs { command, outcome })
		};
		let mut parent = None;
		let lease = match arg.mode {
			tg::process::control::Mode::Resume { lease } => {
				if local_process_control {
					session.index().await?.try_last().await?;
				}
				session
					.get_process_from_index(&id)
					.await?
					.status
					.ok_or_else(|| tg::error!(%id, "missing the process data"))?;
				lease
			},
			tg::process::control::Mode::Start(start) => {
				if shortcut {
					crate::checkpoint!(self.server, "process.control.start.received", process = %id)
						.await;
				}
				let tg::process::control::StartClientRequestArg {
					mut data,
					lease,
					mut options,
					shortcut: shortcut_arg,
				} = start;

				// Require a process on the shortcut path to prove with their tokens that its runner runs its parent and hosts its sandbox, and require a scheduled process not to. Fail the requests that wait for a rejected shortcut start.
				let proven = self
					.verify_process_control_start(shortcut, shortcut_arg, &data)
					.await;
				let (start_parent, sandbox) = match proven {
					Ok(output) => output,
					Err(error) => {
						if shortcut {
							self.spawn_process_control_rejection_task(&id, &error);
						}
						return Err(error);
					},
				};
				parent.clone_from(&start_parent);
				options.tokens.clear();

				// Associate the command with the command sync only on the shortcut path, where the runner pushes the command objects under it.
				if shortcut && let Some(syncs) = &syncs {
					Self::inherit_process_command_authorization_tokens_for_sync(
						&mut data,
						&syncs.command,
					);
				}
				let index_arg = IndexProcessControlArg {
					assign: shortcut,
					data: data.clone(),
					id: id.clone(),
					options,
					parent: start_parent,
					sandbox,
				};
				// Enqueue initialization before acknowledging the start or accepting subsequent requests.
				let result = session.index_process_control(index_arg).boxed().await;
				if let Err(error) = result {
					if shortcut {
						self.spawn_process_control_rejection_task(&id, &error);
					}
					return Err(error);
				}
				lease
			},
		};
		self.server.spawn_publish_process_status_task(&id);

		// Answer the requests for the process for as long as the connection lasts.
		let service_arg = service::Arg {
			parent,
			runner,
			syncs: syncs.is_some(),
		};
		let service = session.spawn_process_control_service_task(&id, service_arg);

		let forwarded_requests = Arc::new(DashSet::new());
		let capacity = self.server.config.process.control_channel_capacity;
		let (sender_high, receiver_high) = tokio::sync::mpsc::channel(capacity);
		let (sender_low, receiver_low) = tokio::sync::mpsc::channel(capacity);
		let config = self.server.config.process.control;
		let mut control = connection::Connection::new(stream, sender_high, sender_low, config);
		let control_sender = control.sender();

		let subject = format!("processes.{id}.control.server");
		let mut requests = self
			.server
			.messenger
			.subscribe::<ServerMessage>(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the message stream"))?;
		let request_sender = control_sender.clone();
		let request_task = Task::spawn({
			let forwarded_requests = forwarded_requests.clone();
			move |_| async move {
				while let Some(message) = requests
					.try_next()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the message"))?
				{
					let message = message.payload.0;
					let acknowledged_request = match &message {
						tg::process::control::ServerMessage::Ack(ack) => Some(ack.id.clone()),
						_ => None,
					};
					if let tg::process::control::ServerMessage::Request(request) = &message {
						forwarded_requests.insert(request.id.clone());
					}
					let low = matches!(
						&message,
						tg::process::control::ServerMessage::Request(
							tg::process::control::ServerRequest {
								arg: tg::process::control::ServerRequestArg::Open(_),
								..
							}
						)
					);
					if low {
						request_sender.send_low(message).await?;
					} else {
						request_sender.send(message).await?;
					}
					if let Some(id) = acknowledged_request {
						forwarded_requests.remove(&id);
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		// Send a refreshed sync when the connection opens and periodically, so that the client always holds an unexpired token.
		let sync_task = syncs.as_ref().map(|syncs| {
			let session = session.clone();
			let control_sender = control_sender.clone();
			let syncs = syncs.clone();
			Task::spawn(move |_| async move {
				session
					.refresh_process_control_sync_task(&control_sender, &syncs)
					.await
					.inspect_err(|error| {
						tracing::error!(error = %error.trace(), "failed to refresh the process control sync");
					})
					.ok();
			})
		});

		let response_task = Task::spawn({
			let session = session.clone();
			let id = id.clone();
			let forwarded_requests = forwarded_requests.clone();
			let syncs = syncs.clone();
			move |_| async move {
				while let Some(message) = control.recv_without_ack().await? {
					match message {
						tg::process::control::ClientMessage::Ack(ack) => {
							if forwarded_requests.contains(&ack.id) {
								session.publish_process_control_ack(&id, ack).await?;
							}
						},
						tg::process::control::ClientMessage::Notification(
							tg::process::control::ClientNotification::ChildSpawned,
						) => {
							session
								.server
								.notifications
								.publish_process_child_spawned(&id)
								.await?;
						},
						tg::process::control::ClientMessage::Notification(
							tg::process::control::ClientNotification::BorrowableCapacity(
								notification,
							),
						) => {
							let runner = notification.runner;
							let scheduler = notification.scheduler;
							let notification = crate::scheduler::Message::Notification(
								crate::scheduler::Notification::BorrowableCapacity(
									crate::scheduler::BorrowableCapacityNotification {
										available: notification.available,
										capacity: notification.capacity,
										heartbeat_index: notification.heartbeat_index,
										parent: notification.parent,
										runner,
									},
								),
							);
							session
								.server
								.messenger
								.publish(
									crate::scheduler::scheduler_server_subject(Some(&scheduler)),
									notification,
								)
								.await
								.map_err(|error| {
									tg::error!(
										!error,
										"failed to publish the borrowable capacity notification"
									)
								})?;
						},
						tg::process::control::ClientMessage::Request(request) => {
							let request_id = request.id;
							control
								.acknowledge_with_priority(
									request_id.clone(),
									tg::process::control::Priority::High,
								)
								.await?;
							match request.arg {
								tg::process::control::ClientRequestArg::Finish(arg) => {
									let result = async {
										let command_sync = syncs
											.as_ref()
											.filter(|_| shortcut)
											.map(|syncs| {
												session.refresh_sync(syncs.command.node.clone())
											})
											.transpose()?;
										let outcome_sync = syncs
											.as_ref()
											.map(|syncs| {
												session.refresh_sync(syncs.outcome.node.clone())
											})
											.transpose()?;
										session
											.finish_process_control_request(
												&id,
												arg,
												command_sync,
												outcome_sync,
											)
											.boxed()
											.await
									}
									.await
									.map(tg::process::control::ServerResponseOutput::Finish);
									let response =
										Self::process_control_server_response(request_id, result);
									control_sender.send(response).await?;
								},
								tg::process::control::ClientRequestArg::Start(_) => {
									let output = tg::process::control::StartServerResponseOutput {};
									let output =
										tg::process::control::ServerResponseOutput::Start(output);
									let response = Self::process_control_server_response(
										request_id,
										Ok(output),
									);
									control_sender.send(response).await?;
								},
							}
						},
						tg::process::control::ClientMessage::Response(mut response) => {
							if let Some(tg::process::control::ClientResponseOutput::Get(output)) =
								&mut response.output
							{
								output.sync = syncs
									.as_ref()
									.map(|syncs| session.refresh_sync(syncs.outcome.node.clone()))
									.transpose()?;
							}
							session
								.publish_process_control_response(&id, response)
								.boxed()
								.await?;
						},
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let stream = crate::control::priority_stream(receiver_high, receiver_low)
			.attach(request_task)
			.attach(response_task)
			.attach(sync_task)
			.attach(service)
			.map(Ok)
			.with_stopper(session.context.stopper.clone())
			.boxed();

		session
			.server
			.messenger
			.publish(connected_subject(&id), Connected { lease })
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to publish the process control connection")
			})?;

		let header = tg::process::control::Header {
			command_sync: syncs.as_ref().map(|syncs| syncs.command.clone()),
			outcome_sync: syncs.map(|syncs| syncs.outcome),
			token,
		};

		crate::checkpoint!(self.server, "process.control.header", process = %id).await;

		Ok(Some((header, stream)))
	}

	async fn refresh_process_control_sync_task(
		&self,
		sender: &connection::Sender,
		syncs: &Syncs,
	) -> tg::Result<()> {
		let period = self.server.config.sync.permission_time_to_live / 2;
		let mut interval = tokio::time::interval(period);
		loop {
			interval.tick().await;
			let command_sync = self.refresh_sync(syncs.command.node.clone())?;
			let outcome_sync = self.refresh_sync(syncs.outcome.node.clone())?;
			let notification = tg::process::control::SyncServerNotification {
				command_sync,
				outcome_sync,
			};
			let message = tg::process::control::ServerMessage::Notification(
				tg::process::control::ServerNotification::Sync(notification),
			);

			// Stop refreshing once the connection closes.
			if sender.send_low(message).await.is_err() {
				return Ok(());
			}
		}
	}

	async fn index_process_control(&self, arg: IndexProcessControlArg) -> tg::Result<()> {
		let IndexProcessControlArg {
			assign,
			data,
			id,
			options,
			parent,
			sandbox,
		} = arg;
		let session = self;
		crate::checkpoint!(self.server, "process.control.index.started", process = %id).await;
		let command_objects = data.command.objects();
		let data = data.without_location_and_tokens_except_sync();
		let sandbox_id = data
			.sandbox
			.as_ref()
			.ok_or_else(|| tg::error!(%id, "the running process has no sandbox"))?;
		let sandbox = if let Some(sandbox) = sandbox {
			Some(
				self.prepare_sandbox_control_index_arg(
					sandbox_id,
					sandbox.created_at,
					sandbox.arg,
					sandbox.creator,
					sandbox.runner,
				)
				.await?,
			)
		} else {
			None
		};
		let account = if let Some(sandbox) = &sandbox {
			sandbox.account.clone()
		} else {
			session
				.usage_account(&tg::Principal::Sandbox(sandbox_id.clone()))
				.await?
		};
		let touched_at = self.server.clock.unix_timestamp()?;
		let location = tg::Location::Local(tg::location::Local {
			region: self.server.config.region.clone(),
		});
		let mut items = vec![tangram_index::batch::Item::PutProcess(
			tangram_index::process::put::Arg {
				cached: false,
				children: None,
				command: Some(
					data.command
						.objects()
						.into_iter()
						.map(|object| object.node)
						.collect(),
				),
				command_id: data.command.command_id()?.into(),
				data: Some(data.clone()),
				error: None,
				id: id.clone(),
				location: Some(location),
				log: None,
				metadata: tg::process::Metadata::default(),
				options,
				output: None,
				parent: parent.clone(),
				permissions: Vec::new(),
				principal: tg::Principal::Process(id.clone()),
				sandbox: Some(sandbox_id.clone()),
				storage: tg::process::storage::Set::NODE,
				time_to_touch: session.server.config.process.time_to_touch,
				touched_at,
			},
		)];
		if let Some(sandbox) = sandbox {
			items.insert(0, tangram_index::batch::Item::PutSandbox(sandbox));
		}
		let permission_arg =
			self.create_process_sandbox_permission_arg(&id, sandbox_id, touched_at)?;
		items.push(tangram_index::batch::Item::PutPermission(permission_arg));
		if let Some(account) = account {
			items.push(tangram_index::batch::Item::PutAccountProcess(
				tangram_index::usage::storage::put::ProcessArg {
					account,
					process: id.clone(),
					touched_at,
				},
			));
		}
		if assign {
			let parent = parent
				.as_ref()
				.ok_or_else(|| tg::error!("a process on the shortcut path must have a parent"))?;
			let destination = id.clone().into();
			let roots = command_objects.into_iter().map(|root| root.map(Into::into));
			let source = tg::Principal::Process(parent.clone());
			items.extend(self.create_capture_permissions_batch_items(
				destination,
				None,
				roots,
				source,
				touched_at,
			)?);
		}

		// Submit the prepared batch without waiting for its index commit.
		let index_arg = tangram_index::batch::Arg { items };
		session
			.server
			.index_batch(index_arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index the process"))?;
		crate::checkpoint!(self.server, "process.control.index.submitted", process = %id).await;

		Ok(())
	}

	/// Verify that a runner derived a process's ID. A start's ID must be recent, and a resume outside the window must name an unfinished process in a sandbox that the runner hosts.
	async fn verify_process_control_proof(
		&self,
		id: &tg::process::Id,
		runner: &tg::runner::Id,
		proof: &tg::process::id::Proof,
		mode: &tg::process::control::Mode,
	) -> tg::Result<()> {
		// Verify that the runner derived the ID.
		proof.verify(id, runner)?;

		// Accept an ID within the window.
		let now = self.server.clock.unix_timestamp()?;
		let now = now.to_u64().unwrap_or_default() * 1000;
		let window = self.server.config.process.id_proof_window;
		let recent = proof.verify_recent(id, runner, now, window);
		let tg::process::control::Mode::Resume { .. } = mode else {
			return recent;
		};
		if recent.is_ok() {
			return Ok(());
		}

		// Accept a resume of an unfinished process in a sandbox that the runner hosts.
		let process = self.get_process_from_index(id).await?;
		if process.status.is_none_or(|status| status.is_finished()) {
			return Err(tg::error!(%id, "the process is not running"));
		}
		let sandbox = process
			.sandbox
			.ok_or_else(|| tg::error!(%id, "the process has no sandbox"))?;
		let sandbox = self
			.server
			.index
			.try_get_sandbox(&sandbox)
			.await?
			.ok_or_else(|| tg::error!(%sandbox, "failed to find the sandbox"))?;
		if sandbox.runner.as_ref() != Some(runner) {
			return Err(tg::error!(
				unauthorized,
				%id,
				"the runner does not host the process's sandbox"
			));
		}

		Ok(())
	}

	/// Verify a start's shortcut proof, returning its parent and the sandbox that it creates.
	async fn verify_process_control_start(
		&self,
		shortcut: bool,
		shortcut_arg: Option<tg::process::control::Shortcut>,
		data: &tg::process::Data,
	) -> tg::Result<(
		Option<tg::process::Id>,
		Option<tg::process::control::SandboxCreate>,
	)> {
		match (shortcut, shortcut_arg) {
			(true, Some(shortcut_arg)) => {
				let tg::process::control::Shortcut { parent, sandbox } = shortcut_arg;
				if let Some(token) = &parent.token {
					let principal = tg::Principal::Process(parent.id.clone());
					self.verify_process_control_start_token(token, principal)
						.await?;
				} else {
					// Verify with the parent's control service that the runner that starts the process started its parent.
					let tg::Principal::Runner(_) = &self.context.principal else {
						return Err(tg::error!(
							unauthorized,
							"a parent without a token requires a runner"
						));
					};
					self.authorize_with_process_control_service(
						&parent.id,
						&self.context.principal,
					)
					.await?;
				}
				let sandbox_id = data.sandbox.as_ref().ok_or_else(|| {
					tg::error!("a process on the shortcut path must have a sandbox")
				})?;
				let principal = tg::Principal::Sandbox(sandbox_id.clone());
				self.verify_process_control_start_token(&sandbox.token, principal)
					.await?;
				Ok((Some(parent.id), sandbox.create))
			},
			(true, None) => Err(tg::error!(
				unauthorized,
				"a process on the shortcut path must prove its parent and sandbox"
			)),
			(false, Some(_)) => Err(tg::error!(
				"a scheduled process's start must not have a shortcut proof"
			)),
			(false, None) => Ok((None, None)),
		}
	}

	/// Verify that a token in a process control start authenticates as the given principal.
	async fn verify_process_control_start_token(
		&self,
		token: &str,
		principal: tg::Principal,
	) -> tg::Result<()> {
		// Accept any token from the root, since the root may act as any principal.
		if matches!(self.context.principal, tg::Principal::Root) {
			return Ok(());
		}

		// Authenticate the token and require it to authenticate as the principal.
		let authentication = self
			.server
			.authenticate(self.context.origin, Some(token))
			.await?;
		if authentication.principal != principal {
			return Err(tg::error!(
				unauthorized,
				%principal,
				"invalid token for a process on the shortcut path"
			));
		}
		Ok(())
	}

	async fn publish_process_control_ack(
		&self,
		id: &tg::process::Id,
		ack: tg::process::control::ClientAck,
	) -> tg::Result<()> {
		let subject = format!("processes.{id}.control.client.{}", ack.id);
		let payload = ClientMessage(tg::process::control::ClientMessage::Ack(ack));
		self.server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to publish the process control acknowledgement"
				)
			})?;

		Ok(())
	}

	async fn publish_process_control_response(
		&self,
		id: &tg::process::Id,
		response: tg::process::control::ClientResponse,
	) -> tg::Result<()> {
		let kind = match response.output.as_ref() {
			Some(tg::process::control::ClientResponseOutput::AcquireLease(_)) => "acquire_lease",
			Some(tg::process::control::ClientResponseOutput::Finish(_)) => "finish",
			Some(tg::process::control::ClientResponseOutput::Get(_)) => "get",
			Some(tg::process::control::ClientResponseOutput::GetChildren(_)) => "get_children",
			Some(tg::process::control::ClientResponseOutput::Open(_)) => "open",
			Some(tg::process::control::ClientResponseOutput::ReleaseLease(_)) => "release_lease",
			Some(tg::process::control::ClientResponseOutput::Signal(_)) => "signal",
			Some(tg::process::control::ClientResponseOutput::Tty(_)) => "tty",
			None => "error",
		};
		let request = response.id.clone();
		crate::checkpoint!(
			self.server,
			"process.control.response.publish",
			kind = %kind,
			process = %id,
			request = request.clone(),
		)
		.await;
		let subject = format!("processes.{id}.control.client.{}", response.id);
		let payload = ClientMessage(tg::process::control::ClientMessage::Response(response));
		self.server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to publish the process control response")
			})?;
		crate::checkpoint!(
			self.server,
			"process.control.response.published",
			kind = %kind,
			process = %id,
			request,
		)
		.await;

		Ok(())
	}

	fn process_control_server_response(
		id: String,
		result: tg::Result<tg::process::control::ServerResponseOutput>,
	) -> tg::process::control::ServerMessage {
		let (error, output) = match result {
			Ok(output) => (None, Some(output)),
			Err(error) => {
				let error = tg::error::Data {
					code: error.code(),
					message: Some(error.to_string()),
					source: Some(tg::Referent::new(
						error.to_data_or_id().map_left(Box::new),
						tg::referent::Options::default(),
					)),
					..Default::default()
				};
				(Some(error), None)
			},
		};
		tg::process::control::ServerMessage::Response(tg::process::control::ServerResponse {
			error,
			id,
			output,
		})
	}

	async fn try_get_process_control_stream_region(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
		region: String,
	) -> tg::Result<
		Option<(
			tg::process::control::Header,
			BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
		)>,
	> {
		let id = arg.id.clone();
		let client = self.get_region_session_for_process(&region).await.map_err(
			|error| tg::error!(!error, region = %region, ?id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::process::control::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client
			.try_get_process_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the control stream"),
			)?;
		let output = output.map(|(header, stream)| {
			let stream = stream.with_stopper(self.context.stopper.clone()).boxed();
			(header, stream)
		});
		Ok(output)
	}

	async fn try_get_process_control_stream_remote(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<
		Option<(
			tg::process::control::Header,
			BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
		)>,
	> {
		let id = arg.id.clone();
		let session = self.get_remote_session_for_process(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, ?id, "failed to get the remote client"),
		)?;
		let context = session.context().clone();
		context.set_token(self.context.token.clone());
		let session = session.client().session(&context);
		let destination = tg::Location::Remote(tg::location::Remote {
			name: remote.clone(),
			region: region.clone(),
		});
		let mut arg = arg;
		if let tg::process::control::Mode::Start(start) = &mut arg.mode {
			start.options.tokens = start.options.tokens.for_location(&destination);
			let data = &mut start.data;
			data.command.options.tokens = data.command.options.tokens.for_location(&destination);
			if let tg::Either::Left(command) = &mut data.command.node {
				**command = command.as_ref().clone().for_location(&destination);
			}
		}
		let stream = stream
			.map_ok(move |mut message| {
				if let tg::process::control::ClientMessage::Request(request) = &mut message
					&& let tg::process::control::ClientRequestArg::Start(start) = &mut request.arg
				{
					start.options.tokens = start.options.tokens.for_location(&destination);
					start.data.command.options.tokens =
						start.data.command.options.tokens.for_location(&destination);
				}
				message
			})
			.boxed();
		let arg = tg::process::control::Arg {
			location: Some(
				tg::Location::Local(tg::location::Local {
					region: region.clone(),
				})
				.into(),
			),
			..arg
		};
		let output = session
			.try_get_process_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, "failed to get the control stream"),
			)?;
		let Some((header, stream)) = output else {
			return Ok(None);
		};
		let stream = stream.with_stopper(self.context.stopper.clone()).boxed();

		Ok(Some((header, stream)))
	}

	pub(crate) async fn request_process_control(
		&self,
		id: &tg::process::Id,
		arg: tg::process::control::ServerRequestArg,
		options: crate::control::Options,
	) -> tg::Result<tg::Result<tg::process::control::ClientResponseOutput>> {
		self.send_process_control_request(id, arg, options)
			.boxed()
			.await?
			.await
	}

	pub(crate) async fn send_process_control_request(
		&self,
		id: &tg::process::Id,
		arg: tg::process::control::ServerRequestArg,
		options: crate::control::Options,
	) -> tg::Result<
		futures::future::BoxFuture<
			'static,
			tg::Result<tg::Result<tg::process::control::ClientResponseOutput>>,
		>,
	> {
		let request_id = crate::control::id();
		let request = ServerMessage(tg::process::control::ServerMessage::Request(
			tg::process::control::ServerRequest {
				arg,
				id: request_id.clone(),
			},
		));
		let arg = crate::control::SendControlRequestArg {
			ack: |id| {
				ServerMessage(tg::process::control::ServerMessage::Ack(
					tg::process::control::ServerAck { id },
				))
			},
			client_subject: format!("processes.{id}.control.client.{request_id}"),
			is_ack: |message: &ClientMessage| {
				matches!(&message.0, tg::process::control::ClientMessage::Ack(_))
			},
			marker: std::marker::PhantomData,
			options,
			request,
			response: |message: ClientMessage| {
				let ClientMessage(tg::process::control::ClientMessage::Response(message)) = message
				else {
					return Ok(None);
				};
				if let Some(error) = message.error {
					let error = tg::Error::try_from(error)
						.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
					return Ok(Some((message.id, Err(error))));
				}
				let Some(output) = message.output else {
					return Err(tg::error!("missing process control response output"));
				};
				Ok(Some((message.id, Ok(output))))
			},
			server_subject: format!("processes.{id}.control.server"),
		};
		self.server.send_control_request(arg).await
	}

	pub(crate) async fn try_get_process_control_stream_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
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
		let tangram_content_type = tg::process::control::TANGRAM_CONTENT_TYPE;
		let output_encoding =
			super::stdio::Encoding::from_accept(accept.as_ref(), tangram_content_type)
				.map_err(|error| tg::error!(argument, !error, "invalid accept type"))?;
		let input_encoding = super::stdio::Encoding::from_content_type(
			content_type
				.as_ref()
				.ok_or_else(|| tg::error!(argument, "missing the content type"))?,
			tangram_content_type,
		)
		.map_err(|error| tg::error!(argument, !error, "invalid content type"))?;

		// Parse the arg.
		let (arg, request) = request
			.arg_with_tangram::<tg::process::control::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.ok_or_else(|| tg::error!(argument, "missing the arg"))?;

		// Decode the request stream.
		let max_frame_size = self.server.config.sync.max_frame_size;
		let stream = super::stdio::decode(request, input_encoding, max_frame_size);

		// Get the response stream.
		let Some((header, stream)) = self
			.try_get_process_control_stream_with_context(arg, stream)
			.boxed()
			.await?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the body.
		let content_type = output_encoding.content_type(tangram_content_type);
		let body = super::stdio::encode(stream, output_encoding, max_frame_size);
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
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes =
			tangram_serialize::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message = tangram_serialize::from_slice(&bytes)
			.map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(message))
	}
}

impl tangram_messenger::Payload for ServerMessage {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes =
			tangram_serialize::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message = tangram_serialize::from_slice(&bytes)
			.map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(message))
	}
}

impl tangram_messenger::Payload for Connected {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes =
			tangram_serialize::to_vec(self).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		tangram_serialize::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}
}
