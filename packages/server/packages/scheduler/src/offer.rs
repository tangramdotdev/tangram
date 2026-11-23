use {
	super::{
		EnqueueSandboxRequestArg, Operation, Scheduler, SchedulerState,
		placement::{Block, Placement, RunnerRef},
		sandbox::State,
	},
	futures::FutureExt as _,
	std::collections::HashMap,
	tangram_client::prelude::*,
	tangram_server_core::{Session, control::accept::Path},
};

pub(super) struct OfferArg<'a> {
	pub(super) capacity: tg::runner::Capacity,
	pub(super) lender: Option<tg::sandbox::Id>,
	pub(super) offer: String,
	pub(super) request: &'a EnqueueSandboxRequestArg,
	pub(super) runner: &'a RunnerRef,
	pub(super) scheduler: &'a tg::scheduler::Id,
	pub(super) timeout: std::time::Duration,
}

/// A runner's answer to an offer of a sandbox.
pub(super) enum Outcome {
	/// The runner holds the sandbox.
	Accepted,

	/// The runner does not have the capacity for the sandbox.
	Declined,

	/// The runner held the boot and it ended, because the runner gave it up or a later boot replaced it.
	Ended,

	/// The runner failed to create the sandbox.
	Failed(tg::Error),

	/// The runner did not answer the offer.
	Unanswered(tg::Error),
}

/// What this scheduler does with a runner's answer to the current offer of a boot.
pub(super) enum Decision {
	/// Record that the runner holds the boot.
	Accept,

	/// Forget the boot as the end of a boot that the runner held, and offer it to no other runner.
	End,

	/// Return the boot to the queue, and count the failure if the runner failed to create the sandbox.
	Requeue { failure: Option<tg::Error> },

	/// Send the offer again, because only the runner's answer shows whether the runner holds the boot.
	Resend { error: tg::Error },
}

/// Decide what this scheduler does with a runner's answer to the current offer of a boot. Only a runner's no and its failure to create the sandbox return the boot to the queue, because a runner that answers that the boot ended held it.
pub(super) fn decide_offer_outcome(outcome: Outcome) -> Decision {
	match outcome {
		Outcome::Accepted => Decision::Accept,
		Outcome::Declined => Decision::Requeue { failure: None },
		Outcome::Ended => Decision::End,
		Outcome::Failed(error) => Decision::Requeue {
			failure: Some(error),
		},
		Outcome::Unanswered(error) => Decision::Resend { error },
	}
}

/// Offer a sandbox to a runner, and wait for the runner's answer.
#[tracing::instrument(name = "sandbox.offer", level = "debug", skip_all, fields(runner = %arg.runner.id, sandbox = %arg.request.sandbox, scheduler = %arg.scheduler))]
pub(super) async fn send_offer(session: &Session, arg: OfferArg<'_>) -> Outcome {
	let OfferArg {
		capacity,
		lender,
		offer,
		request,
		runner,
		scheduler,
		timeout,
	} = arg;
	tracing::info!(lender = lender.as_ref().map(ToString::to_string), runner = %runner.id, sandbox = %request.sandbox, scheduler = %scheduler, "offering the sandbox to a runner");
	let arg = tg::runner::control::ServerRequestArg::OfferBoot(
		tg::runner::control::OfferBootServerRequestArg {
			arg: request.arg.clone(),
			boot: request.boot,
			capacity,
			creator: request.creator.clone(),
			lender,
			owner: request.owner.clone(),
			parent: request.parent.clone(),
			placed: request.runner.is_some(),
			process: request.process.clone(),
			sandbox: request.sandbox.clone(),
			sequence: tangram_index::sandbox::put::FIRST_HOLDER_WRITE,
			token: request.token.clone(),
		},
	);

	// Send the offer once and wait for the answer until the timeout, because the scheduler sends the offer again with the same ID if the runner does not answer. The request's own timeout bounds each wait for an acknowledgement or an answer and restarts after an acknowledgement, so the outer timeout bounds the whole wait.
	let retry = tangram_futures::retry::Options {
		max_retries: 0,
		..session.server.config.control.request_retry.clone().into()
	};
	let options = tangram_server_core::control::Options { retry, timeout };
	let future = session
		.server
		.components
		.runner
		.send_runner_control_request(
			session,
			&runner.id,
			scheduler,
			runner.connection,
			offer,
			arg,
			options,
		)
		.boxed();
	let result = tokio::time::timeout(timeout, future)
		.await
		.map_err(|error| tg::error!(!error, "timed out waiting for the runner to answer"))
		.and_then(std::convert::identity);
	if result.is_err() {
		tangram_server_core::checkpoint!(session.server, "scheduler.sandbox.offer.unanswered", sandbox = %request.sandbox)
			.await;
	}
	let result = match result {
		Ok(result) => result,
		Err(error) => {
			let error = tg::error!(!error, sandbox = %request.sandbox, runner = %runner.id, "failed to send the offer of the sandbox to the runner");
			return Outcome::Unanswered(error);
		},
	};
	let result = result
		.map_err(|error| {
			tg::error!(!error, sandbox = %request.sandbox, runner = %runner.id, "the runner failed to create the sandbox")
		})
		.and_then(|output| {
			output
				.try_unwrap_offer_boot()
				.map_err(|_| tg::error!("expected an offer boot response"))
				.map(|output| output.answer)
		});
	match result {
		Ok(tg::runner::control::Answer::Ended) => Outcome::Ended,
		Ok(tg::runner::control::Answer::No) => Outcome::Declined,
		Ok(tg::runner::control::Answer::Yes) => Outcome::Accepted,
		Err(error) => Outcome::Failed(error),
	}
}

/// Add a request to a sum of requests, which accounts for each request's maximum as its minimum when it has none.
pub(super) fn add(capacity: &mut tg::runner::Capacity, value: tg::runner::Capacity) {
	let max = capacity
		.cpu
		.accounted_max()
		.millis()
		.saturating_add(value.cpu.accounted_max().millis());
	let min = capacity
		.cpu
		.min
		.millis()
		.saturating_add(value.cpu.min.millis());
	capacity.cpu = tg::sandbox::Cpu::new(
		tg::sandbox::Amount::from_millis(min),
		tg::sandbox::Amount::from_millis(max),
	);
	for (resource, value) in [
		(&mut capacity.memory, value.memory),
		(&mut capacity.storage, value.storage),
	] {
		let max = resource
			.accounted_max()
			.saturating_add(value.accounted_max());
		let min = resource.min.saturating_add(value.min);
		*resource = tg::sandbox::Resource::new(min, max);
	}
	capacity.sandboxes = capacity.sandboxes.saturating_add(value.sandboxes);
}

/// Subtract a request from a sum of requests.
pub(super) fn subtract(capacity: &mut tg::runner::Capacity, value: tg::runner::Capacity) {
	let max = capacity
		.cpu
		.accounted_max()
		.millis()
		.saturating_sub(value.cpu.accounted_max().millis());
	let min = capacity
		.cpu
		.min
		.millis()
		.saturating_sub(value.cpu.min.millis());
	capacity.cpu = tg::sandbox::Cpu::new(
		tg::sandbox::Amount::from_millis(min),
		tg::sandbox::Amount::from_millis(max),
	);
	for (resource, value) in [
		(&mut capacity.memory, value.memory),
		(&mut capacity.storage, value.storage),
	] {
		let max = resource
			.accounted_max()
			.saturating_sub(value.accounted_max());
		let min = resource.min.saturating_sub(value.min);
		*resource = tg::sandbox::Resource::new(min, max);
	}
	capacity.sandboxes = capacity.sandboxes.saturating_sub(value.sandboxes);
}

impl SchedulerState {
	/// Record a runner's yes to a boot, which is the answer to an offer or the report of the boot to a restarted scheduler, and write the handoff. Every yes goes through this function, so that no yes is recorded without the handoff. The runner holds the boot from its yes, the sandbox leaves the queue, and the waiting enqueues and dequeues are answered. The scheduler keeps the sandbox's request until its control connection is established.
	pub(super) fn accept_sandbox(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		runner: RunnerRef,
	) {
		// Remove the sandbox from the queue, which an offered sandbox keeps its place in.
		if matches!(
			self.sandboxes.entries[id].state,
			State::Offered { .. } | State::Queued
		) {
			self.remove_queued_sandbox(id);
		}

		// Record that the runner holds the boot.
		let entry = self.sandboxes.entries.get_mut(id).unwrap();
		entry.blocked = HashMap::default();
		let dequeue_requests = std::mem::take(&mut entry.dequeue_requests);
		let parent = entry.parent.clone();
		let request = entry.request().clone();
		if let Some(parent) = &parent {
			self.insert_child(parent, id, &runner.id);
		}
		let runner_id = runner.id.clone();
		self.sandboxes.entries.get_mut(id).unwrap().state = State::Accepted { runner };
		self.stop_serving_process_holder(id);

		// Publish the sandbox's status, so that a reader that this scheduler answered as the boot's holder reads again from the runner.
		scheduler
			.server
			.components
			.sandbox
			.spawn_publish_sandbox_status_task(&scheduler.server, id);

		// Write the handoff, which is this scheduler's last write of the boot.
		self.hand_off_sandbox(scheduler, request, runner_id);

		// Reply to the waiting dequeues that the sandbox was not dequeued, and to the waiting enqueues that the sandbox is enqueued.
		scheduler.send_dequeue_responses(dequeue_requests, false);
		self.finish_accept(scheduler, id, Path::Handled, Ok(()));
	}

	/// Write the handoff of a boot that a runner accepted, which removes the boot from this scheduler's queue. Write it again until it applies, because the boot stays in this scheduler's saved queue until the handoff removes it.
	fn hand_off_sandbox(
		&mut self,
		scheduler: &Scheduler,
		request: EnqueueSandboxRequestArg,
		runner: tg::runner::Id,
	) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				loop {
					let result = scheduler
						.write_sandbox_handoff(&request, &runner)
						.await;
					let Err(error) = result else {
						break;
					};
					tracing::error!(error = %error.trace(), sandbox = %request.sandbox, %runner, "failed to record the handoff of the sandbox");
					tokio::time::sleep(super::recovery::READ_RETRY_INTERVAL).await;
				}
				Operation::Finished {
					context: "failed to record the handoff of the sandbox",
					result: Ok(()),
				}
			}
			.boxed(),
		);
	}

	/// Handle a runner's answer to an offer of a sandbox.
	pub(super) fn handle_offer_outcome(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		placement: &Placement,
		outcome: Outcome,
	) {
		self.sandboxes.offers = self.sandboxes.offers.saturating_sub(1);

		// Ignore the answer to an offer that is not the sandbox's current offer, such as an answer on a runner connection that the runner's reconnect replaced, after which the offer is sent to the new connection, and an answer on the connection of a runner that restarted since, whose first connection decides the offer from the index.
		let current = self.sandboxes.entries.get(id).is_some_and(|sandbox| {
			matches!(&sandbox.state, State::Offered { placement: current, .. } if current == placement)
		}) && self
			.runners
			.entries
			.get(&placement.runner().id)
			.is_some_and(|runner| runner.connection == placement.runner().connection);
		if !current {
			return;
		}
		let runner = placement.runner().clone();
		let sandbox = self.sandboxes.entries.get_mut(id).unwrap();
		let failure = match decide_offer_outcome(outcome) {
			// Record that the runner holds the sandbox.
			Decision::Accept => {
				let connected = matches!(
					sandbox.state,
					State::Offered {
						connected: true,
						..
					}
				);
				self.remove_reservation(id, placement, true);
				self.accept_sandbox(scheduler, id, runner);

				// Drop the request of a sandbox whose control connection is established, and keep serving the spawns into the sandbox, which this scheduler sends to its runner.
				if connected {
					self.sandboxes.entries.get_mut(id).unwrap().request = None;
				}
				return;
			},

			// Forget a boot that the runner held and that ended, as the end of a boot that the runner held, offer it to no other runner, and write its end if the index does not record one.
			Decision::End => {
				tracing::info!(sandbox = %id, runner = %runner.id, "the runner answered that the boot ended");
				let request = sandbox.request().clone();
				self.fail_claimed_sandbox(scheduler, request, runner.id.clone());
				self.end_held_sandbox(scheduler, id, Some(placement.clone()));
				return;
			},

			// Count the failure of a runner that failed to create the sandbox.
			Decision::Requeue { failure } => {
				if let Some(error) = &failure {
					tracing::error!(error = %error.trace(), sandbox = %id, runner = %runner.id, "the runner failed to create the sandbox");
					sandbox.failures += 1;
				}
				failure
			},

			// Send the offer again, and keep the reservation and the waiting dequeues until the runner answers.
			Decision::Resend { error } => {
				tracing::warn!(error = %error.trace(), sandbox = %id, runner = %runner.id, "the runner did not answer the offer of the sandbox");
				self.retry_sandbox_offer(scheduler, id);
				return;
			},
		};

		// Return the sandbox to the queue.
		let dequeue_requests = std::mem::take(&mut sandbox.dequeue_requests);
		let failures = sandbox.failures;
		self.remove_reservation(id, placement, false);
		self.requeue_sandbox(id);
		if !dequeue_requests.is_empty() {
			// End a sandbox that a dequeue waits for.
			self.end_sandbox_and_log(scheduler, id, tg::sandbox::Status::Destroyed, None);
			scheduler.send_dequeue_responses(dequeue_requests, true);
		} else if let Some(source) = failure
			&& failures >= scheduler.config.max_offer_failures
		{
			// Give up on a sandbox whose offers failed too many times.
			let error = tg::error!(
				!source,
				sandbox = %id,
				%failures,
				"failed to create the sandbox after too many attempts"
			);
			tracing::error!(error = %error.trace(), "giving up on the sandbox");
			let discarded = self.discard_sandbox(scheduler, id, &error);
			scheduler.publish_sandbox_discarded(self, discarded);
		} else {
			// Try another runner at once, and not this runner until its next heartbeat.
			if matches!(placement, Placement::Regular { .. })
				&& let Some(entry) = self.runners.entries.get(&runner.id)
			{
				let block = Block {
					connection: entry.connection,
					heartbeat_index: entry.heartbeat_index,
				};
				self.sandboxes
					.entries
					.get_mut(id)
					.unwrap()
					.blocked
					.insert(runner.id.clone(), block);
			}
			self.try_offer_sandbox(scheduler, id);
		}
	}

	/// Try to place a sandbox that no runner could fit again once its timeout expires, which discards it if no runner can fit it still.
	pub(super) fn handle_retry_sandbox(&mut self, scheduler: &Scheduler, id: &tg::sandbox::Id) {
		let Some(sandbox) = self.sandboxes.entries.get_mut(id) else {
			return;
		};
		if !matches!(sandbox.state, State::Queued) || !sandbox.retrying {
			return;
		}
		sandbox.retrying = false;
		self.try_offer_sandbox(scheduler, id);
	}

	pub(super) fn remove_reservation(
		&mut self,
		id: &tg::sandbox::Id,
		placement: &Placement,
		commit: bool,
	) {
		let runner_ref = placement.runner();
		let Some(runner) = self.runners.entries.get_mut(&runner_ref.id) else {
			return;
		};
		if runner.connection != runner_ref.connection {
			return;
		}
		let Some(reserved) = runner.reservations.remove(id) else {
			return;
		};
		subtract(&mut runner.reserved, reserved);
		if commit {
			add(&mut runner.committed, reserved);
		}
	}

	pub(super) fn try_offer_sandbox(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
	) -> bool {
		// Offer nothing while a restarted scheduler waits for its runners to report, because a runner may hold a boot that the scheduler would otherwise offer again.
		if self.recovery.is_some() {
			return false;
		}
		let Some(sandbox) = self.sandboxes.entries.get(id) else {
			return false;
		};
		if !matches!(sandbox.state, State::Queued) {
			return false;
		}
		let Some(placement) = self.find_placement(scheduler, sandbox) else {
			// Keep the sandbox queued if a runner could satisfy it once it has capacity.
			if self.placeable(sandbox) {
				self.sandboxes
					.entries
					.get_mut(id)
					.unwrap()
					.unplaceable_since = None;
				self.hit_unplaced_checkpoint(scheduler, id, true);
				return false;
			}

			// Keep a sandbox that no runner can fit queued if the configuration waits for one.
			let Some(timeout) = scheduler.config.unplaceable_timeout else {
				self.hit_unplaced_checkpoint(scheduler, id, false);
				return false;
			};

			// Discard the sandbox once no runner has been able to fit it for the timeout.
			let now = scheduler.server.clock.instant();
			let sandbox = self.sandboxes.entries.get_mut(id).unwrap();
			let since = *sandbox.unplaceable_since.get_or_insert(now);
			let elapsed = now - since;
			if elapsed >= timeout {
				let error = tg::error!(sandbox = %id, "no runners available");
				tracing::error!(error = %error.trace(), "giving up on the sandbox");
				let discarded = self.discard_sandbox(scheduler, id, &error);
				scheduler.publish_sandbox_discarded(self, discarded);
				return false;
			}

			// Check again when the timeout expires, in case no runner becomes available.
			if !sandbox.retrying {
				sandbox.retrying = true;
				let sandbox = id.clone();
				let sleep = scheduler
					.server
					.clock
					.sleep(timeout.saturating_sub(elapsed));
				self.operations.push(
					async move {
						sleep.await;
						Operation::RetrySandbox { sandbox }
					}
					.boxed(),
				);
			}

			return false;
		};
		let capacity = sandbox.capacity;
		let runner_ref = placement.runner().clone();

		// Reserve the sandbox's capacity on the runner, which for a borrowed sandbox is only the capacity that it acquires in addition to what it borrows.
		let reserved = match &placement {
			Placement::Borrowed { parent, .. } => {
				let borrowable = self
					.parents
					.0
					.get_mut(parent)
					.and_then(|parent| parent.borrowable.as_mut())
					.unwrap();
				borrowable.available = false;
				let (_, acquired) = borrowable.capacity.try_borrow(capacity).unwrap();
				acquired
			},
			Placement::Regular { .. } => capacity,
		};
		let runner = self.runners.entries.get_mut(&runner_ref.id).unwrap();
		add(&mut runner.reserved, reserved);
		runner.reservations.insert(id.clone(), reserved);
		if let Some(parent) = self.sandboxes.entries[id].parent.clone() {
			self.insert_child(&parent, id, &runner_ref.id);
		}
		self.sandboxes.entries.get_mut(id).unwrap().state = State::Offered {
			connected: false,
			offer: tangram_server_core::control::id(),
			placement,
			retries: 0,
		};

		// Offer the boot to the runner.
		self.offer_sandbox(scheduler, id);

		true
	}

	/// Hit a checkpoint in an operation, if checkpoints are enabled, once the scheduler has decided to keep a sandbox queued. The sandbox is placeable if a runner could fit it once the runner has capacity.
	fn hit_unplaced_checkpoint(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		placeable: bool,
	) {
		if scheduler.server.checkpoints.is_none() {
			return;
		}
		let scheduler = scheduler.clone();
		let sandbox = id.clone();
		self.operations.push(
			async move {
				tangram_server_core::checkpoint!(scheduler.server, "scheduler.sandbox.unplaced", placeable, sandbox = %sandbox)
					.await;
				Operation::Checkpoint
			}
			.boxed(),
		);
	}

	/// Send the offer of a boot to its runner, whose answer completes the offer.
	pub(super) fn offer_sandbox(&mut self, scheduler: &Scheduler, id: &tg::sandbox::Id) {
		let sandbox = &self.sandboxes.entries[id];
		let State::Offered {
			offer, placement, ..
		} = &sandbox.state
		else {
			return;
		};
		let capacity = sandbox.capacity;
		let offer = offer.clone();
		let placement = placement.clone();
		let request = sandbox.request().clone();
		self.sandboxes.offers += 1;
		let session = scheduler.server.session(&scheduler.server.context);
		let scheduler_id = scheduler.id.clone();
		let timeout = scheduler.config.offer_timeout;
		let lender = match &placement {
			Placement::Borrowed { parent, .. } => Some(parent.clone()),
			Placement::Regular { .. } => None,
		};
		let sandbox = id.clone();
		self.operations.push(
			async move {
				let runner = placement.runner();
				let arg = OfferArg {
					capacity,
					lender,
					offer,
					request: &request,
					runner,
					scheduler: &scheduler_id,
					timeout,
				};
				let outcome = send_offer(&session, arg).boxed().await;
				Operation::OfferBoot {
					outcome,
					placement,
					sandbox,
				}
			}
			.boxed(),
		);
	}

	/// Send an offer that its runner did not answer again after a delay, with no limit, until the runner answers or is declared dead.
	pub(super) fn retry_sandbox_offer(&mut self, scheduler: &Scheduler, id: &tg::sandbox::Id) {
		let Some(sandbox) = self.sandboxes.entries.get_mut(id) else {
			return;
		};
		let State::Offered { offer, retries, .. } = &mut sandbox.state else {
			return;
		};
		*retries = retries.saturating_add(1);
		let delay =
			tangram_futures::retry::delay_for_attempt(*retries, &scheduler.config.offer_retry);
		let offer = offer.clone();
		let sandbox = id.clone();
		self.operations.push(
			async move {
				tokio::time::sleep(delay).await;
				Operation::RetrySandboxOffer { offer, sandbox }
			}
			.boxed(),
		);
	}

	/// Send an offer again, unless the runner answered it or the runner was removed in the meantime.
	pub(super) fn handle_retry_sandbox_offer(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		offer: &str,
	) {
		let current = self
			.sandboxes
			.entries
			.get(id)
			.is_some_and(|sandbox| match &sandbox.state {
				State::Offered {
					offer: current,
					placement,
					..
				} => {
					let runner = placement.runner();
					current == offer
						&& self
							.runners
							.entries
							.get(&runner.id)
							.is_some_and(|entry| entry.connection == runner.connection)
				},
				State::Accepted { .. } | State::Queued | State::Terminal { .. } => false,
			});
		if current {
			self.offer_sandbox(scheduler, id);
		}
	}

	/// Send the unanswered offers of a runner that reconnected again to its new connection, whose entry keeps their reservations. The runner is the same program and answers an offer that it already answered from its memory of its reply, so that no offer is returned to the queue and no boot is offered to a second runner while the first may hold it.
	pub(super) fn resend_runner_offers(
		&mut self,
		scheduler: &Scheduler,
		runner: &tg::runner::Id,
		connection: u64,
	) {
		let sandboxes = self.runners.entries[runner]
			.reservations
			.keys()
			.cloned()
			.collect::<Vec<_>>();
		for sandbox in sandboxes {
			// Point the offer at the runner's new connection, unless it was made on the new connection.
			let Some(State::Offered {
				placement, retries, ..
			}) = self
				.sandboxes
				.entries
				.get_mut(&sandbox)
				.map(|entry| &mut entry.state)
			else {
				continue;
			};
			if placement.runner().connection == connection {
				continue;
			}
			placement.set_connection(connection);
			*retries = 0;

			// Send the offer again.
			self.offer_sandbox(scheduler, &sandbox);
		}
	}
}
