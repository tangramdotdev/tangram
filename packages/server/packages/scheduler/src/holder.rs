use {
	super::{
		EnqueueSandboxRequestArg, HolderMessage, Operation, Scheduler, SchedulerState,
		sandbox::{HolderTasks, State},
	},
	futures::{FutureExt as _, StreamExt as _, future},
	tangram_client::prelude::*,
	tangram_messenger::Messenger as _,
};

impl SchedulerState {
	/// Serve the requests to the holder of a sandbox that this scheduler holds, and to the holder of the sandbox's queued process, and keep serving the spawns into the sandbox for as long as this scheduler knows the sandbox.
	pub(super) fn serve_sandbox_holder(
		&mut self,
		scheduler: &Scheduler,
		sandbox: &tg::sandbox::Id,
		process: Option<tg::process::Id>,
	) {
		// Serve a sandbox once, and record it before its subscriptions are established, so that a request is not served twice.
		if self.sandboxes.holders.contains_key(sandbox) {
			return;
		}
		self.sandboxes.holders.insert(sandbox.clone(), None);
		let scheduler = scheduler.clone();
		let sandbox = sandbox.clone();
		self.operations.push(
			async move {
				let result = scheduler
					.serve_sandbox_holder(&sandbox, process.as_ref())
					.await;
				Operation::ServeSandboxHolder { result, sandbox }
			}
			.boxed(),
		);
	}

	/// Keep the tasks that serve a sandbox's holder, unless this scheduler stopped serving it in the meantime.
	pub(super) fn handle_serve_sandbox_holder_completion(
		&mut self,
		sandbox: &tg::sandbox::Id,
		result: tg::Result<HolderTasks>,
	) {
		let mut tasks = match result {
			Ok(tasks) => tasks,
			Err(error) => {
				tracing::error!(error = %error.trace(), %sandbox, "failed to serve the sandbox's holder");
				self.sandboxes.holders.remove(sandbox);
				return;
			},
		};
		let accepted = self
			.sandboxes
			.entries
			.get(sandbox)
			.is_some_and(|entry| matches!(entry.state, State::Accepted { .. }));
		if accepted {
			tasks.process = None;
		}
		if let Some(holder) = self.sandboxes.holders.get_mut(sandbox) {
			*holder = Some(tasks);
		}
	}

	/// Stop serving the requests to the holder of a sandbox's queued process once a runner holds the process.
	pub(super) fn stop_serving_process_holder(&mut self, sandbox: &tg::sandbox::Id) {
		if let Some(Some(tasks)) = self.sandboxes.holders.get_mut(sandbox) {
			tasks.process = None;
		}
	}

	/// Stop serving the requests to the holder of a sandbox that this scheduler no longer holds.
	pub(super) fn stop_serving_sandbox_holder(&mut self, sandbox: &tg::sandbox::Id) {
		self.sandboxes.holders.remove(sandbox);
	}

	/// Stop serving the holders of the sandboxes that this scheduler neither knows nor holds for recovery.
	pub(super) fn prune_sandbox_holders(&mut self) {
		let sandboxes = self
			.sandboxes
			.holders
			.keys()
			.filter(|sandbox| {
				let known = self.sandboxes.entries.contains_key(*sandbox);
				!(known || self.is_recovering(sandbox))
			})
			.cloned()
			.collect::<Vec<_>>();
		for sandbox in sandboxes {
			self.sandboxes.holders.remove(&sandbox);
		}
	}

	/// Answer a request to the holder of a sandbox that this scheduler holds, and reply that this scheduler does not hold a sandbox that it has offered to a runner.
	pub(super) fn handle_sandbox_holder_message(
		&mut self,
		scheduler: &Scheduler,
		message: HolderMessage,
	) {
		let (request, sandbox) = match message {
			HolderMessage::Connected { sandbox } => {
				// Drop the request of a sandbox whose runner serves it, and keep serving the spawns into the sandbox, which this scheduler sends to the sandbox's runner. Stop serving a sandbox that this scheduler does not know.
				let entry = self.sandboxes.entries.get_mut(&sandbox);
				let held = match entry.map(|entry| (&mut entry.state, &mut entry.request)) {
					Some((State::Accepted { .. }, request)) => {
						*request = None;
						self.stop_serving_process_holder(&sandbox);
						true
					},
					Some((State::Offered { connected, .. }, _)) => {
						*connected = true;
						true
					},
					Some((State::Queued | State::Terminal { .. }, _)) => true,
					None => false,
				};
				if !held {
					self.stop_serving_sandbox_holder(&sandbox);
				}
				return;
			},
			HolderMessage::Ended {
				boot,
				runner,
				sandbox,
			} => {
				let ended = super::report::EndedBoot {
					declared: true,
					processes: std::collections::BTreeSet::new(),
					runner,
				};
				self.sandboxes.ended.insert(sandbox, boot, ended);
				return;
			},
			HolderMessage::Held { sender } => {
				sender.send(self.get_held_sandboxes()).ok();
				return;
			},
			HolderMessage::ProcessRequest { request, sandbox } => {
				self.handle_process_holder_request(scheduler, &sandbox, request);
				return;
			},
			HolderMessage::Request { request, sandbox } => (request, sandbox),
		};
		let tg::sandbox::control::ServerRequest { arg, id } = request;

		// Ignore a request for a sandbox that this scheduler does not serve.
		if !self.sandboxes.holders.contains_key(&sandbox) {
			return;
		}
		// Leave a spawn that this scheduler sends to a runner with the boot to the runner.
		if let tg::sandbox::control::ServerRequestArg::SpawnProcess(arg) = &arg
			&& arg.boot.is_some()
		{
			return;
		}
		let state = self
			.sandboxes
			.entries
			.get(&sandbox)
			.map(|entry| (&entry.state, entry.boot));
		match state {
			// Hold a spawn into a sandbox that a runner holds, and offer the process to the runner with the boot, which accepts it and finishes it as failed if it is not running the boot.
			Some((State::Accepted { .. }, boot))
				if matches!(arg, tg::sandbox::control::ServerRequestArg::SpawnProcess(_)) =>
			{
				let tg::sandbox::control::ServerRequestArg::SpawnProcess(mut arg) = arg else {
					unreachable!()
				};
				arg.boot = Some(boot);

				// Remember that this scheduler offered the process into the boot, so that the runner's reports of the boot may finish it.
				if let Some(process) = &arg.process.id {
					let entry = self.sandboxes.entries.get_mut(&sandbox).unwrap();
					entry.processes.insert(process.clone());
				}
				let spawn = super::spawn::Spawn { arg, id, sandbox };
				self.spawn_process(scheduler, spawn, true);
				return;
			},

			// Reply that this scheduler is not ready for a change of a sandbox that it has offered to a runner, because the offer is unanswered and a change never rides with an offer. The runner carries out the change once it accepts the offer, and this scheduler does so if the runner declines it.
			Some((State::Offered { .. }, _))
				if matches!(
					arg,
					tg::sandbox::control::ServerRequestArg::Destroy(_)
						| tg::sandbox::control::ServerRequestArg::SpawnProcess(_)
						| tg::sandbox::control::ServerRequestArg::Start(_)
						| tg::sandbox::control::ServerRequestArg::Stop(_)
				) =>
			{
				self.publish_sandbox_holder_retry(
					scheduler,
					sandbox,
					id,
					tg::control::Retry::NotReady,
				);
				return;
			},

			// Reply that this scheduler does not hold a sandbox that a runner accepted, and let the index answer a read of a sandbox that it has offered to a runner.
			None | Some((State::Accepted { .. } | State::Offered { .. }, _)) => {
				self.publish_sandbox_holder_retry(
					scheduler,
					sandbox,
					id,
					tg::control::Retry::NotHolder,
				);
				return;
			},

			Some((State::Queued, _)) => (),

			// Answer a request for a sandbox that this scheduler ended with the sandbox's terminal state, which the index may not have recorded yet.
			Some((State::Terminal { .. }, _)) => {
				self.handle_terminal_sandbox_holder_request(scheduler, sandbox, id, &arg);
				return;
			},
		}

		// Reply that this scheduler holds a queued sandbox but is not ready for a spawn, which only a runner can carry out once the sandbox starts.
		if matches!(arg, tg::sandbox::control::ServerRequestArg::SpawnProcess(_)) {
			self.publish_sandbox_holder_retry(scheduler, sandbox, id, tg::control::Retry::NotReady);
			return;
		}
		let request = self
			.sandboxes
			.entries
			.get_mut(&sandbox)
			.unwrap()
			.request_mut();

		match arg {
			// Destroy a queued sandbox by ending it.
			tg::sandbox::control::ServerRequestArg::Destroy(arg) => {
				let status = tg::sandbox::Status::Destroyed;
				let error = arg.error.map(tg::Either::Left);
				let write = self.end_sandbox(scheduler, &sandbox, status, error).boxed();
				self.queue.wake();
				let output = tg::sandbox::control::ClientResponseOutput::Destroy(
					tg::sandbox::control::DestroyClientResponseOutput { destroyed: true },
				);
				self.publish_sandbox_holder_response(scheduler, sandbox, id, Some(write), output);
			},

			// Answer a get with the queued boot.
			tg::sandbox::control::ServerRequestArg::Get(_) => {
				let data = Self::queued_sandbox_get_output(scheduler, request);
				let output = tg::sandbox::control::ClientResponseOutput::Get(
					tg::sandbox::control::GetClientResponseOutput { data },
				);
				self.publish_sandbox_holder_response(scheduler, sandbox, id, None, output);
			},

			// Answer a get of the processes with the queued process.
			tg::sandbox::control::ServerRequestArg::GetProcesses(arg) => {
				let processes = request
					.process
					.as_ref()
					.and_then(|process| process.id.clone())
					.into_iter()
					.collect::<Vec<_>>();
				let length = u64::try_from(processes.len()).unwrap();
				let processes = processes
					.into_iter()
					.skip(usize::try_from(arg.position).unwrap())
					.take(usize::try_from(arg.length).unwrap())
					.collect();
				let output = tg::sandbox::control::ClientResponseOutput::GetProcesses(
					tg::sandbox::control::GetProcessesClientResponseOutput {
						length,
						processes,
						status: tg::sandbox::Status::Starting,
					},
				);
				self.publish_sandbox_holder_response(scheduler, sandbox, id, None, output);
			},

			tg::sandbox::control::ServerRequestArg::SpawnProcess(_) => unreachable!(),

			// Answer a start of a queued boot with the boot, which is starting.
			tg::sandbox::control::ServerRequestArg::Start(_) => {
				let data = Self::queued_sandbox_get_output(scheduler, request);
				let output = tg::sandbox::control::ClientResponseOutput::Start(
					tg::sandbox::control::StartClientResponseOutput { data },
				);
				self.publish_sandbox_holder_response(scheduler, sandbox, id, None, output);
			},

			// Stop a queued boot by ending it as stopped, because a stop never rides with an offer.
			tg::sandbox::control::ServerRequestArg::Stop(_) => {
				let output = tg::sandbox::control::ClientResponseOutput::Stop(
					tg::sandbox::control::StopClientResponseOutput { stopped: true },
				);
				let status = tg::sandbox::Status::Stopped;
				let write = self.end_sandbox(scheduler, &sandbox, status, None).boxed();
				self.queue.wake();
				self.publish_sandbox_holder_response(scheduler, sandbox, id, Some(write), output);
			},
		}
	}

	/// Answer a request to the holder of a sandbox that this scheduler ended. A get, a get of the processes, and a start are answered with the sandbox's terminal state, so that a start of a stopped sandbox reserves the next boot, and a stop of a stopped sandbox and a destroy of a destroyed sandbox have nothing left to do. Any other request waits for the index, which answers it once it has recorded the terminal state.
	pub(super) fn handle_terminal_sandbox_holder_request(
		&mut self,
		scheduler: &Scheduler,
		sandbox: tg::sandbox::Id,
		id: String,
		arg: &tg::sandbox::control::ServerRequestArg,
	) {
		let entry = &self.sandboxes.entries[&sandbox];
		let State::Terminal { error, status, .. } = &entry.state else {
			return;
		};
		let status = *status;
		let data = Self::sandbox_get_output(scheduler, entry.request(), status, error.clone());
		let output = match arg {
			tg::sandbox::control::ServerRequestArg::Destroy(_) if status.is_destroyed() => {
				tg::sandbox::control::ClientResponseOutput::Destroy(
					tg::sandbox::control::DestroyClientResponseOutput { destroyed: false },
				)
			},
			tg::sandbox::control::ServerRequestArg::Get(_) => {
				tg::sandbox::control::ClientResponseOutput::Get(
					tg::sandbox::control::GetClientResponseOutput { data },
				)
			},
			tg::sandbox::control::ServerRequestArg::GetProcesses(_) => {
				tg::sandbox::control::ClientResponseOutput::GetProcesses(
					tg::sandbox::control::GetProcessesClientResponseOutput {
						length: 0,
						processes: Vec::new(),
						status,
					},
				)
			},
			tg::sandbox::control::ServerRequestArg::Start(_) => {
				tg::sandbox::control::ClientResponseOutput::Start(
					tg::sandbox::control::StartClientResponseOutput { data },
				)
			},
			tg::sandbox::control::ServerRequestArg::Stop(_) if status.is_stopped() => {
				tg::sandbox::control::ClientResponseOutput::Stop(
					tg::sandbox::control::StopClientResponseOutput { stopped: false },
				)
			},
			tg::sandbox::control::ServerRequestArg::Destroy(_)
			| tg::sandbox::control::ServerRequestArg::SpawnProcess(_)
			| tg::sandbox::control::ServerRequestArg::Stop(_) => {
				self.publish_sandbox_holder_retry(
					scheduler,
					sandbox,
					id,
					tg::control::Retry::NotReady,
				);
				return;
			},
		};
		self.publish_sandbox_holder_response(scheduler, sandbox, id, None, output);
	}

	/// Create the output of a get of a queued sandbox.
	pub(super) fn queued_sandbox_get_output(
		scheduler: &Scheduler,
		request: &EnqueueSandboxRequestArg,
	) -> tg::sandbox::get::Output {
		Self::sandbox_get_output(scheduler, request, tg::sandbox::Status::Starting, None)
	}

	/// Create the output of a get of a sandbox that this scheduler holds.
	pub(super) fn sandbox_get_output(
		scheduler: &Scheduler,
		request: &EnqueueSandboxRequestArg,
		status: tg::sandbox::Status,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	) -> tg::sandbox::get::Output {
		let mut data = tangram_server_scheduler_core::sandbox_data(request, status);
		data.error = error;
		let location = tg::Location::Local(tg::location::Local {
			region: scheduler.server.config.region.clone(),
		});
		tg::sandbox::get::Output {
			data,
			location: Some(location),
			tokens: tg::authorization::Tokens::default(),
		}
	}

	/// Answer a request for a lease of a queued process, reply that this scheduler is not ready for any other request of a queued process, which only a runner can carry out once the process starts, and reply that this scheduler does not hold a process once it has offered the process's sandbox to a runner. The offer carries the leases, so a process whose sandbox was offered to a runner takes leases over process control once it connects.
	pub(super) fn handle_process_holder_request(
		&mut self,
		scheduler: &Scheduler,
		sandbox: &tg::sandbox::Id,
		request: tg::process::control::ServerRequest,
	) {
		let tg::process::control::ServerRequest { arg, id } = request;

		// Ignore a request for a process whose sandbox this scheduler does not serve.
		if !self.sandboxes.holders.contains_key(sandbox) {
			return;
		}

		// Reply that this scheduler is not ready for a request of the process of a sandbox that it ended, which the index answers once it has recorded the finished process.
		if let Some(entry) = self.sandboxes.entries.get(sandbox)
			&& matches!(entry.state, State::Terminal { .. })
		{
			let Some(process) = entry
				.request()
				.process
				.as_ref()
				.and_then(|process| process.id.clone())
			else {
				return;
			};
			self.publish_process_holder_retry(scheduler, process, id, tg::control::Retry::NotReady);
			return;
		}
		let entry = self
			.sandboxes
			.entries
			.get(sandbox)
			.filter(|entry| matches!(entry.state, State::Queued));
		let Some(process) = entry.and_then(|entry| {
			let process = entry.request().process.as_ref()?.id.clone()?;
			Some(process)
		}) else {
			let Some(process) = self.try_get_served_process(sandbox) else {
				return;
			};
			self.publish_process_holder_retry(
				scheduler,
				process,
				id,
				tg::control::Retry::NotHolder,
			);
			return;
		};
		let request = self
			.sandboxes
			.entries
			.get_mut(sandbox)
			.unwrap()
			.request_mut();
		let queued = request.process.as_mut().unwrap();

		match arg {
			// Add a lease to the queued process.
			tg::process::control::ServerRequestArg::AcquireLease(_) => {
				let lease = tg::process::Lease::new();
				queued.leases.push(lease);
				let data = queued.data.clone();
				let request = request.clone();
				let output = tg::process::control::ClientResponseOutput::AcquireLease(
					tg::process::control::AcquireLeaseClientResponseOutput {
						data,
						lease: Some(lease),
					},
				);
				let scheduler_ = scheduler.clone();
				let write =
					async move { scheduler_.persist_sandbox(&request, false).await }.boxed();
				self.publish_process_holder_response(scheduler, process, id, Some(write), output);
			},

			// Release a lease of the queued process, and end the process and its sandbox if no leases remain.
			tg::process::control::ServerRequestArg::ReleaseLease(arg) => {
				let lease = arg.lease;
				let output = |released| {
					tg::process::control::ClientResponseOutput::ReleaseLease(
						tg::process::control::ReleaseLeaseClientResponseOutput { released },
					)
				};
				let position = queued.leases.iter().position(|value| *value == lease);
				let last = position.is_none() && queued.lease == lease && queued.leases.is_empty();
				if let Some(position) = position {
					queued.leases.remove(position);
				} else if queued.lease == lease
					&& let Some(next) = queued.leases.pop()
				{
					queued.lease = next;
				} else if !last {
					self.publish_process_holder_response(
						scheduler,
						process,
						id,
						None,
						output(false),
					);
					return;
				}
				let scheduler_ = scheduler.clone();
				let write = if last {
					let error = tg::error::Data {
						code: Some(tg::error::Code::Cancellation),
						message: Some("the process was canceled".to_owned()),
						..Default::default()
					};
					let status = tg::sandbox::Status::Destroyed;
					let error = Some(tg::Either::Left(error));
					let write = self.end_sandbox(scheduler, sandbox, status, error).boxed();
					self.queue.wake();
					write
				} else {
					let request = request.clone();
					async move { scheduler_.persist_sandbox(&request, false).await }.boxed()
				};
				self.publish_process_holder_response(
					scheduler,
					process,
					id,
					Some(write),
					output(true),
				);
			},

			tg::process::control::ServerRequestArg::Finish(_)
			| tg::process::control::ServerRequestArg::Get(_)
			| tg::process::control::ServerRequestArg::GetChildren(_)
			| tg::process::control::ServerRequestArg::Open(_)
			| tg::process::control::ServerRequestArg::Signal(_)
			| tg::process::control::ServerRequestArg::Tty(_) => {
				self.publish_process_holder_retry(
					scheduler,
					process,
					id,
					tg::control::Retry::NotReady,
				);
			},
		}
	}

	/// Publish this scheduler's response to a request to the holder of a sandbox. A response that follows a write is acknowledged first, and is sent once the write is durable.
	pub(super) fn publish_sandbox_holder_response(
		&mut self,
		scheduler: &Scheduler,
		sandbox: tg::sandbox::Id,
		id: String,
		write: Option<futures::future::BoxFuture<'static, tg::Result<()>>>,
		output: tg::sandbox::control::ClientResponseOutput,
	) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let component = &scheduler.server.components.sandbox;
				let result = async {
					let mut error = None;
					let mut output = Some(output);
					if let Some(write) = write {
						let ack = tg::sandbox::control::ClientAck { id: id.clone() };
						component
							.publish_sandbox_holder_ack(
								&scheduler.server,
								&sandbox,
								tangram_server_core::sandbox::Holder::Scheduler,
								ack,
							)
							.await?;
						if let Err(source) = write.await {
							error = Some(tg::error::Data {
								message: Some(source.to_string()),
								..Default::default()
							});
							output = None;
						}
					}
					let response = tg::sandbox::control::ClientResponse {
						error,
						id,
						output,
						retry: None,
					};
					component
						.publish_sandbox_holder_response(
							&scheduler.server,
							&sandbox,
							tangram_server_core::sandbox::Holder::Scheduler,
							response,
						)
						.await
				}
				.await;
				Operation::Finished {
					context: "failed to answer the request to the sandbox's holder",
					result,
				}
			}
			.boxed(),
		);
	}

	/// Publish this scheduler's reply to a request to the holder of a sandbox that asks the request's sender to send the request again.
	pub(super) fn publish_sandbox_holder_retry(
		&mut self,
		scheduler: &Scheduler,
		sandbox: tg::sandbox::Id,
		id: String,
		retry: tg::control::Retry,
	) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let response = tg::sandbox::control::ClientResponse::with_retry(id, retry);
				let result = scheduler
					.server
					.components
					.sandbox
					.publish_sandbox_holder_response(
						&scheduler.server,
						&sandbox,
						tangram_server_core::sandbox::Holder::Scheduler,
						response,
					)
					.await;
				Operation::Finished {
					context: "failed to answer the request to the sandbox's holder",
					result,
				}
			}
			.boxed(),
		);
	}

	/// Get the request of a sandbox that this scheduler has a request for.
	pub(super) fn try_get_request_mut(
		&mut self,
		sandbox: &tg::sandbox::Id,
	) -> Option<&mut EnqueueSandboxRequestArg> {
		let tracked = self
			.sandboxes
			.entries
			.get(sandbox)
			.is_some_and(|entry| entry.request.is_some());
		if tracked {
			return self
				.sandboxes
				.entries
				.get_mut(sandbox)
				.and_then(|entry| entry.request.as_mut());
		}
		self.try_get_recovering_request_mut(sandbox)
	}

	/// Get the queued process of a sandbox whose holder this scheduler serves.
	pub(super) fn try_get_served_process(
		&mut self,
		sandbox: &tg::sandbox::Id,
	) -> Option<tg::process::Id> {
		self.try_get_request_mut(sandbox)?
			.process
			.as_ref()?
			.id
			.clone()
	}

	/// Publish this scheduler's response to a request to the holder of a process. A response that follows a write is acknowledged first, and is sent once the write is durable.
	pub(super) fn publish_process_holder_response(
		&mut self,
		scheduler: &Scheduler,
		process: tg::process::Id,
		id: String,
		write: Option<futures::future::BoxFuture<'static, tg::Result<()>>>,
		output: tg::process::control::ClientResponseOutput,
	) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let component = &scheduler.server.components.process;
				let result = async {
					let mut error = None;
					let mut output = Some(output);
					if let Some(write) = write {
						let ack = tg::process::control::ClientAck { id: id.clone() };
						component
							.publish_process_holder_ack(&scheduler.server, &process, ack)
							.await?;
						if let Err(source) = write.await {
							error = Some(tg::error::Data {
								message: Some(source.to_string()),
								..Default::default()
							});
							output = None;
						}
					}
					let response = tg::process::control::ClientResponse {
						error,
						id,
						output,
						retry: None,
					};
					component
						.publish_process_holder_response(&scheduler.server, &process, response)
						.await
				}
				.await;
				Operation::Finished {
					context: "failed to answer the request to the process's holder",
					result,
				}
			}
			.boxed(),
		);
	}

	/// Publish this scheduler's reply to a request to the holder of a process that asks the request's sender to send the request again.
	pub(super) fn publish_process_holder_retry(
		&mut self,
		scheduler: &Scheduler,
		process: tg::process::Id,
		id: String,
		retry: tg::control::Retry,
	) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let response = tg::process::control::ClientResponse::with_retry(id, retry);
				let result = scheduler
					.server
					.components
					.process
					.publish_process_holder_response(&scheduler.server, &process, response)
					.await;
				Operation::Finished {
					context: "failed to answer the request to the process's holder",
					result,
				}
			}
			.boxed(),
		);
	}
}

impl Scheduler {
	/// Subscribe to the requests to the holder of a sandbox and of its queued process, and to the sandbox's control connection, and send each to the scheduling task.
	pub(super) async fn serve_sandbox_holder(
		&self,
		sandbox: &tg::sandbox::Id,
		process: Option<&tg::process::Id>,
	) -> tg::Result<HolderTasks> {
		// Serve the requests to the sandbox's holder.
		let handle = {
			let sender = self.holder_sender.clone();
			let sandbox = sandbox.clone();
			move |message: tg::sandbox::control::ServerMessage| {
				if let tg::sandbox::control::ServerMessage::Request(request) = message {
					let sandbox = sandbox.clone();
					sender
						.send(HolderMessage::Request { request, sandbox })
						.ok();
				}
				future::ready(Ok(())).boxed()
			}
		};
		let requests = self
			.server
			.components
			.sandbox
			.serve_sandbox_holder(
				&self.server,
				sandbox,
				tangram_server_core::sandbox::Holder::Scheduler,
				Box::new(handle),
			)
			.await?;

		// Serve the requests to the holder of the sandbox's queued process.
		let process = if let Some(process) = process {
			let handle = {
				let sender = self.holder_sender.clone();
				let sandbox = sandbox.clone();
				move |message: tg::process::control::ServerMessage| {
					if let tg::process::control::ServerMessage::Request(request) = message {
						let sandbox = sandbox.clone();
						sender
							.send(HolderMessage::ProcessRequest { request, sandbox })
							.ok();
					}
					future::ready(Ok(())).boxed()
				}
			};
			let requests = self
				.server
				.components
				.process
				.serve_process_holder(&self.server, process, Box::new(handle))
				.await?;
			Some(requests)
		} else {
			None
		};

		// Observe the sandbox's control connection.
		let subject = tangram_server_sandbox_common::control::connected_subject(sandbox);
		let mut connected = self
			.server
			.messenger
			.subscribe::<()>(subject)
			.await
			.map_err(
				|error| tg::error!(!error, %sandbox, "failed to subscribe to the sandbox control connection"),
			)?;
		let connected = tangram_futures::task::Task::spawn({
			let sender = self.holder_sender.clone();
			let sandbox = sandbox.clone();
			move |_| async move {
				while connected.next().await.is_some() {
					let sandbox = sandbox.clone();
					sender.send(HolderMessage::Connected { sandbox }).ok();
				}
				Ok(())
			}
		});
		let tasks = HolderTasks {
			connected,
			process,
			requests,
		};

		Ok(tasks)
	}
}
