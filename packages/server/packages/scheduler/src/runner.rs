use {
	super::{
		AddRunnerRequestArg, AddRunnerResponseOutput, HeartbeatNotification, Operation,
		RemoveRunnerRequestArg, RemoveRunnerResponseOutput, Scheduler, SchedulerState,
		sandbox::State,
	},
	crate::Component,
	futures::FutureExt as _,
	std::{
		collections::{BTreeMap, BTreeSet, HashMap, HashSet},
		ops::ControlFlow,
		time::Duration,
	},
	tangram_cache::log,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_server_core::Server,
};

#[cfg(test)]
pub(super) mod tests;

/// The sandboxes of a runner that its scheduler ends, because it declared the runner dead or the runner restarted.
pub(super) struct EndRunnerSandboxesArg {
	/// The sandboxes whose boots the runner accepted and that this scheduler forgot, which the end covers although the index may not record them on the runner, because the runner did not write them yet.
	pub accepted: Vec<tg::sandbox::Id>,

	/// The checkpoint that the routine hits once it has read the runner's sandboxes.
	pub checkpoint: &'static str,

	/// Whether the runner was deleted, which never runs a sandbox again, so that the end destroys every sandbox of the runner.
	pub deleted: bool,

	/// The receiver that the end tells of each boot that it ends, which is the scheduling task, so that it remembers the boot as ended and tells the runner when the runner connects again, or `None` if the runner does not hold the boots, because it restarted or was deleted.
	pub ended: Option<tokio::sync::mpsc::UnboundedSender<super::HolderMessage>>,

	/// The error that the ended sandboxes and their processes record.
	pub error: tg::error::Data,

	/// The boots that the runner holds.
	pub held: Vec<tg::runner::control::ArgSandbox>,

	/// The sandboxes whose current boot the scheduler holds.
	pub queued: Vec<tg::sandbox::Id>,

	pub runner: tg::runner::Id,

	/// The scheduling task, which the end asks after it reads the index for the sandboxes whose current boot the scheduler holds, or `None` before the task runs.
	pub scheduler: Option<tokio::sync::mpsc::UnboundedSender<super::HolderMessage>>,

	/// The sandboxes whose offers to the runner are unanswered and whose boots the index shows that the runner wrote, which the end covers although the scheduler's queue has them.
	pub written: Vec<tg::sandbox::Id>,
}

/// A boot of a runner that this scheduler declared dead, which the scheduler ends.
pub(super) struct EndDeadRunnerBootArg<'a> {
	pub boot: u64,

	/// Whether the sandbox cannot stay stopped once the boot ends, such as a sandbox with a zero ttl, a sandbox of a deleted runner, or a sandbox that a dequeue or a destroy waits for, which the end then destroys.
	pub destroy: bool,

	/// The error that the ended sandbox and its processes record.
	pub error: tg::error::Data,

	/// The latest write of the boot's request that this scheduler started, which follows the boot's first record, and whose write and indexing the end waits for, or `None` if the caller read the boot from the index.
	pub persist: Option<super::sandbox::Write>,

	pub runner: &'a tg::runner::Id,

	pub sandbox: &'a tg::sandbox::Id,

	/// The scheduling task, which the end tells of the boot that it ends, so that the task remembers the boot as ended, or `None` if the caller is the scheduling task, which remembers the boot itself, or if no scheduling task runs.
	pub task: Option<&'a tokio::sync::mpsc::UnboundedSender<super::HolderMessage>>,
}

/// A runner's connection with its inputs, which this scheduler computes when the connection arrives and applies once the connection is added.
pub(super) struct Connection {
	/// The boots that the runner accepted and no longer reports, which the end of a restarted runner's boots covers and which the connection forgets.
	pub accepted: Vec<(tg::sandbox::Id, u64)>,

	/// The reported boots that this scheduler declared ended, which the connection tells the runner.
	pub ended: Vec<tg::runner::control::AbandonBoot>,

	/// Whether the connection is the runner's first since it started, or a connection of a runner whose restart this scheduler is still handling.
	pub first: bool,

	/// The number of the connection.
	pub number: u64,

	/// The sandboxes whose current boot this scheduler holds.
	pub queued: Vec<tg::sandbox::Id>,

	/// Whether the runner reports to this restarted scheduler for the first time.
	pub recovering: bool,

	pub runner: tg::runner::Id,

	/// The boots that the runner reports.
	pub sandboxes: Vec<tg::runner::control::ArgSandbox>,

	/// The unanswered offers to a runner that restarted, and the boots that it claimed in its reports to this restarted scheduler, which the connection decides from the index.
	pub unanswered: Vec<UnansweredOffer>,
}

/// The output of a runner's connection.
pub(super) struct AddRunnerOutput {
	/// The decisions of the unanswered offers to a runner that restarted and of the boots that it claimed, from the index, which the connection's end and its application both follow.
	pub decided: BTreeMap<tg::sandbox::Id, super::recovery::SavedBoot>,

	/// The reported boots and processes that the index does not have on the runner's first report to this restarted scheduler, which this scheduler cancels.
	pub lost: Lost,

	pub output: AddRunnerResponseOutput,

	pub owner: Option<tg::Id>,
}

/// The boots and processes that a runner reports to a restarted scheduler and that the index does not have, which the scheduler lost before its first write of them.
#[derive(Default)]
pub(super) struct Lost {
	pub processes: Vec<tg::process::Id>,
	pub sandboxes: Vec<tg::runner::control::ArgSandbox>,
}

/// An unanswered offer to a runner that restarted, which the runner's first connection decides from the index.
pub(super) struct UnansweredOffer {
	pub boot: u64,

	/// The offer, or `None` for a boot that the runner claimed in its report to this restarted scheduler, which this scheduler holds for recovery.
	pub offer: Option<String>,

	pub sandbox: tg::sandbox::Id,
}

/// Why this scheduler declares a runner dead, with the ID of the request to answer.
enum Removal {
	/// The runner was deleted, and never returns.
	Deleted { id: String },

	/// The runner's connection expired or its stream ended because it shut down.
	Removed { id: Option<String> },
}

pub(super) struct Runners {
	pub entries: HashMap<tg::runner::Id, Runner, tg::id::BuildHasher>,

	/// The number of the next runner connection, which this scheduler keeps only in memory and starts at a random number, so that a connection to an earlier run of the scheduler is not mistaken for a current one.
	pub next_connection: u64,
}

pub(super) struct Runner {
	/// The runner's sandboxes that lend capacity to their children.
	pub borrowable: HashSet<tg::sandbox::Id, tg::id::BuildHasher>,

	/// The capacity that the runner's last heartbeat reported.
	pub capacity: tg::runner::control::Capacity,

	/// The capacity of the sandboxes that the runner accepted since its last heartbeat.
	pub committed: tg::runner::Capacity,

	pub connection: u64,

	/// Whether the scheduler failed to record the runner's connection, in which case the runner takes no sandboxes, and expires with its sandboxes unless it connects again.
	pub connection_failed: bool,

	pub heartbeat_at: tangram_server_core::clock::Instant,

	pub heartbeat_index: u64,

	pub host: String,

	pub owner: Option<tg::Id>,

	/// The capacity that each sandbox that is offered to the runner reserves, which for a borrowed sandbox is only the capacity that it acquires in addition to what it borrows.
	pub reservations: HashMap<tg::sandbox::Id, tg::runner::Capacity, tg::id::BuildHasher>,

	/// The sum of the reservations.
	pub reserved: tg::runner::Capacity,

	/// Whether the runner restarted and this scheduler has not finished handling its first connection, until which the runner is sent nothing and every connection of the runner is handled as a first connection.
	pub restarting: bool,
}

/// Decide the answer to a runner's report of a sandbox that it started on the shortcut path. A report on a connection that a reconnect replaced and a report that this scheduler cannot authorize yet are sent again, and a report that this scheduler refuses is final.
pub(super) fn decide_created_sandbox(
	current: bool,
	authorization: super::report::Authorization,
) -> tg::Result<ControlFlow<super::report::Authorized, tg::control::Retry>> {
	match authorization {
		super::report::Authorization::Authorized(authorized) if current => {
			Ok(ControlFlow::Break(authorized))
		},
		super::report::Authorization::Authorized(_) | super::report::Authorization::NotReady => {
			Ok(ControlFlow::Continue(tg::control::Retry::NotReady))
		},
		super::report::Authorization::Refused(error) => Err(error),
	}
}

/// Decide each unanswered offer to a runner that restarted and each boot that it claimed from the index's record of its sandbox, with the one decision of a saved boot, so that an offer whose boot ended is never offered again, even if the end was written by an earlier connection that did not apply.
pub(super) fn decide_unanswered_offers(
	unanswered: &[UnansweredOffer],
	indexed: Vec<Option<tangram_index::sandbox::Sandbox>>,
) -> BTreeMap<tg::sandbox::Id, super::recovery::SavedBoot> {
	std::iter::zip(unanswered, indexed)
		.map(|(offer, indexed)| {
			let decision = super::recovery::decide_saved_boot(offer.boot, indexed.as_ref());
			(offer.sandbox.clone(), decision)
		})
		.collect()
}

impl Runner {
	/// Whether the runner's connection succeeded, this scheduler handled the runner's restart, and the runner has sent a heartbeat and takes sandboxes.
	pub fn ready(&self) -> bool {
		!self.connection_failed
			&& !self.restarting
			&& self.heartbeat_index > 0
			&& self.capacity.total.sandboxes != 0
	}

	/// The capacity that the runner has now, which is what its last heartbeat reported without what the scheduler has reserved and committed since.
	pub fn available(&self) -> tg::runner::Capacity {
		let factor = self.capacity.overcommit;
		self.capacity
			.available
			.subtract(self.reserved, factor)
			.subtract(self.committed, factor)
	}

	/// Whether the runner has the capacity for a request now.
	pub fn fits(&self, capacity: tg::runner::Capacity) -> bool {
		self.available()
			.contains(capacity, self.capacity.overcommit)
	}
}

impl Runners {
	pub fn new() -> Self {
		Self {
			entries: HashMap::default(),
			next_connection: tangram_server_core::service::start() >> 1,
		}
	}

	pub fn expired(
		&self,
		now: tangram_server_core::clock::Instant,
		time_to_live: Duration,
	) -> Vec<(tg::runner::Id, u64)> {
		self.entries
			.iter()
			.filter(|(_, runner)| now - runner.heartbeat_at > time_to_live)
			.map(|(id, runner)| (id.clone(), runner.connection))
			.collect()
	}
}

impl SchedulerState {
	/// Handle a runner's connection. Compute its inputs, which change nothing that the connection decides, and add the runner in an operation, whose completion applies the connection.
	pub(super) fn handle_add_runner_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		mut request: AddRunnerRequestArg,
	) {
		let connection = self.connect_runner(&mut request, scheduler.server.clock.instant());
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler.add_runner(request, &connection).await;
				Operation::AddRunner {
					connection,
					id,
					result,
				}
			}
			.boxed(),
		);
	}

	/// Number a runner's connection, replace the runner's entry with one for the connection that keeps the reservations of the runner's unanswered offers and the time of the runner's last sign of life, and get the connection's inputs. Nothing else that the connection decides changes until the connection is added and applied, and only an applied connection counts as a sign of life, so that a runner whose connections never apply expires.
	pub(super) fn connect_runner(
		&mut self,
		request: &mut AddRunnerRequestArg,
		now: tangram_server_core::clock::Instant,
	) -> Connection {
		// Number the runner's connection.
		let connection = self.runners.next_connection;
		self.runners.next_connection = connection.wrapping_add(1);
		tracing::info!(runner = %request.runner, connection, "connecting the runner");

		// Handle a connection of a runner whose restart this scheduler is still handling as a first connection, so that the runner is sent nothing until its restart is handled.
		let restarting = self
			.runners
			.entries
			.get(&request.runner)
			.is_some_and(|runner| runner.restarting);
		request.first = request.first || restarting;

		// Get whether the runner reports to this restarted scheduler, whose first report the connection checks against the index.
		let recovering = self
			.recovery
			.as_ref()
			.is_some_and(|recovery| recovery.has_runner(&request.runner));

		// Get the reported boots that this scheduler declared ended, which it tells the runner from its memory, and which the runner gives up.
		let ended = self.get_declared_ended_boots(&request.runner, &request.sandboxes);

		// Get the boots that the runner accepted and no longer reports.
		let accepted = self.get_unreported_sandboxes(&request.runner, &request.sandboxes);

		// Replace the runner's entry, and keep the reservations of the runner's unanswered offers and the time of its last sign of life on it.
		let (heartbeat_at, offers) = self
			.remove_runner_entry(&request.runner)
			.map_or((now, HashMap::default()), |runner| {
				(runner.heartbeat_at, runner.reservations)
			});
		let runner = Runner {
			borrowable: HashSet::default(),
			capacity: request.capacity,
			committed: tg::runner::Capacity::default(),
			connection,
			connection_failed: false,
			heartbeat_at,
			heartbeat_index: 0,
			host: request.host.clone(),
			owner: None,
			reservations: HashMap::default(),
			reserved: tg::runner::Capacity::default(),
			restarting: request.first,
		};
		self.runners.entries.insert(request.runner.clone(), runner);
		self.keep_unanswered_offers(&request.runner, offers);
		self.queue.wake();

		// Get the unanswered offers to a runner that restarted, and the boots that it claimed in its reports to this restarted scheduler, which its first connection decides from the index.
		let unanswered = if request.first {
			self.get_unanswered_offers(&request.runner)
		} else {
			Vec::new()
		};

		// Get the sandboxes whose current boot this scheduler holds, which the end on the runner's first connection leaves to this scheduler.
		let queued = self.get_held_sandboxes();

		Connection {
			accepted,
			ended,
			first: request.first,
			number: connection,
			queued,
			recovering,
			runner: request.runner.clone(),
			sandboxes: request.sandboxes.clone(),
			unanswered,
		}
	}

	/// Apply a runner's connection once its addition succeeded and it is still the runner's current connection, which is the only place where a runner's connection changes what this scheduler knows about the runner's boots. Record the runner's restart as handled, forget the ended boots of a runner that restarted, forget the accepted boots that the runner no longer reports, record the runner's report for the recovery of a restarted scheduler, track the reported boots that this scheduler authorizes, decide or send again the unanswered offers, cancel the lost boots and processes, and remove the directories of the destroying sandboxes.
	pub(super) fn apply_runner_connection(
		&mut self,
		scheduler: &Scheduler,
		connection: Connection,
		output: AddRunnerOutput,
	) {
		let Connection {
			accepted,
			ended,
			first,
			number: connection,
			runner: id,
			sandboxes,
			unanswered,
			..
		} = connection;
		let AddRunnerOutput {
			decided,
			lost,
			owner,
			..
		} = output;

		// Record that the runner's restart is handled, so that the runner is sent work again, and count the connection as a sign of life.
		let entry = self.runners.entries.get_mut(&id).unwrap();
		entry.heartbeat_at = scheduler.server.clock.instant();
		entry.owner = owner;
		entry.restarting = false;

		// Forget the ended boots of a runner that restarted, which reports none of them again, except the reported boots that this scheduler declared ended after the connection arrived, which the connection was not told, and forget the boots that the runner no longer reports.
		if first {
			let kept = sandboxes
				.iter()
				.filter(|sandbox| {
					!ended
						.iter()
						.any(|ended| ended.id == sandbox.id && ended.boot == sandbox.boot)
				})
				.map(|sandbox| (sandbox.id.clone(), sandbox.boot))
				.collect::<Vec<_>>();
			self.sandboxes.ended.forget_runner(&id, &kept);
		}
		self.forget_accepted_sandboxes(scheduler, &id, &accepted);

		// Record the runner's report for the recovery of a restarted scheduler, and get the boots that the scheduler rebuilt from the index on the runner.
		let recovered = self.handle_recovery_report(scheduler, &id, first, &sandboxes, &decided);

		// Track the reported boots that this scheduler authorizes, which are the ones that it gave to the runner, the ones that it rebuilt from the index on the runner after it restarted, and the ones whose IDs the runner proves that it derived, and act on no other reported boot. Leave the ones that a restarted scheduler holds for recovery, which the runner's report claims and which the end of the recovery accepts, and the ones that ended.
		for sandbox in &sandboxes {
			let ended = ended
				.iter()
				.any(|ended| ended.id == sandbox.id && ended.boot == sandbox.boot);
			if self.is_recovering(&sandbox.id) || ended {
				continue;
			}
			let recovered = recovered.iter().find(|recovered| {
				recovered.sandbox == sandbox.id && recovered.boot == sandbox.boot
			});
			let report = super::report::Report {
				boot: Some(sandbox.boot),
				processes: Vec::new(),
				proof: sandbox.proof.as_ref(),
				runner: &id,
				sandbox: &sandbox.id,
			};
			let authorized = recovered.is_some()
				|| matches!(
					self.authorize_runner_report(&report),
					super::report::Authorization::Authorized(super::report::Authorized {
						held: super::report::Held::Accepted | super::report::Held::Derived,
						..
					})
				);
			if !authorized {
				tracing::warn!(sandbox = %sandbox.id, boot = sandbox.boot, runner = %id, "ignoring a reported boot that the scheduler did not give to the runner");
				continue;
			}
			let runner = super::placement::RunnerRef {
				connection,
				id: id.clone(),
			};
			let processes = recovered.map_or_default(|recovered| recovered.processes.clone());
			self.track_accepted_sandbox(
				scheduler,
				&sandbox.id,
				sandbox.boot,
				None,
				processes,
				runner,
			);
		}

		// Remember the rebuilt boots that a runner that did not restart no longer reports, which ended while this scheduler was down, so that this scheduler authorizes the runner's reports of their ends.
		if !first {
			for recovered in recovered {
				let reported = sandboxes.iter().any(|sandbox| {
					sandbox.id == recovered.sandbox && sandbox.boot == recovered.boot
				});
				if reported {
					continue;
				}
				let ended = super::report::EndedBoot {
					declared: false,
					processes: recovered.processes,
					runner: id.clone(),
				};
				self.sandboxes
					.ended
					.insert(recovered.sandbox, recovered.boot, ended);
			}
		}

		// Decide the unanswered offers to a runner that restarted from the index, and send the unanswered offers again to a runner that reconnects, which is the same program and answers them from its memory of its replies.
		if first {
			self.handle_unanswered_offers(scheduler, &id, unanswered, &decided);
		} else {
			self.resend_runner_offers(scheduler, &id, connection);
		}

		// Cancel the lost boots and processes, and remove the directories of the destroying sandboxes.
		self.cancel_lost_sandboxes(scheduler, &id, connection, lost);
		self.read_destroying_sandboxes(scheduler, &id);
		self.queue.wake();
	}

	/// Get the reported boots that this scheduler declared ended for the runner, which are the only boots that the runner's connection is told ended.
	pub(super) fn get_declared_ended_boots(
		&self,
		runner: &tg::runner::Id,
		sandboxes: &[tg::runner::control::ArgSandbox],
	) -> Vec<tg::runner::control::AbandonBoot> {
		sandboxes
			.iter()
			.filter(|sandbox| {
				self.sandboxes
					.ended
					.is_declared(&sandbox.id, sandbox.boot, runner)
			})
			.map(|sandbox| tg::runner::control::AbandonBoot {
				boot: sandbox.boot,
				id: sandbox.id.clone(),
			})
			.collect()
	}

	/// Track a sandbox that a runner started on the shortcut path, so that this scheduler knows the sandbox and sends the spawns into it to the runner. Authorize the report with the proof that the runner derived the sandbox's ID, and ignore a report of a boot that already ended, so that a report that arrives after the boot's end does not track the boot again.
	pub(super) fn handle_runner_sandbox_created_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: tangram_server_scheduler_core::RunnerSandboxCreatedRequestArg,
	) {
		let current = self
			.runners
			.entries
			.get(&request.runner)
			.is_some_and(|runner| runner.connection == request.connection);
		let report = super::report::Report {
			boot: Some(request.boot),
			processes: Vec::new(),
			proof: Some(&request.proof),
			runner: &request.runner,
			sandbox: &request.sandbox,
		};
		let authorization = self.authorize_runner_report(&report);
		let result = match decide_created_sandbox(current, authorization) {
			Ok(ControlFlow::Break(authorized)) => {
				if matches!(authorized.held, super::report::Held::Derived) {
					let runner = super::placement::RunnerRef {
						connection: request.connection,
						id: request.runner,
					};
					self.track_accepted_sandbox(
						scheduler,
						&request.sandbox,
						request.boot,
						None,
						BTreeSet::new(),
						runner,
					);
				}
				let output = tangram_server_scheduler_core::RunnerSandboxCreatedResponseOutput {
					retry: None,
				};
				Ok(super::ResponseOutput::RunnerSandboxCreated(output))
			},
			Ok(ControlFlow::Continue(retry)) => {
				let output = tangram_server_scheduler_core::RunnerSandboxCreatedResponseOutput {
					retry: Some(retry),
				};
				Ok(super::ResponseOutput::RunnerSandboxCreated(output))
			},
			Err(error) => Err(error),
		};
		let response = super::request::Response::new(id, result);
		scheduler.send_response(response);
	}

	/// Carry out the removal of a destroying sandbox's directory, which this scheduler carries out until the runner confirms it or is declared dead, and reply at once.
	pub(super) fn handle_destroy_runner_sandbox_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: super::DestroyRunnerSandboxRequestArg,
	) {
		self.remove_destroying_sandbox(scheduler, request.sandbox, request.runner, request.error);
		let output = super::DestroyRunnerSandboxResponseOutput {};
		let response = super::request::Response::new(
			id,
			Ok(super::ResponseOutput::DestroyRunnerSandbox(output)),
		);
		scheduler.send_response(response);
	}

	/// Ask a runner to remove the directory of a destroying sandbox, whose report records the destruction. Record the destruction if the runner is declared dead, because the runner never removes the directory, and the runner removes it once it reads the destruction from the index if the runner returns. Wait for a runner that is connected but not ready, or that this scheduler waits for after a restart.
	pub(super) fn remove_destroying_sandbox(
		&mut self,
		scheduler: &Scheduler,
		sandbox: tg::sandbox::Id,
		runner: tg::runner::Id,
		error: Option<tg::error::Data>,
	) {
		let connection = self
			.runners
			.entries
			.get(&runner)
			.filter(|runner| runner.ready())
			.map(|runner| runner.connection);
		let waiting = self
			.recovery
			.as_ref()
			.is_some_and(|recovery| recovery.has_runner(&runner))
			|| self.runners.entries.contains_key(&runner);

		// Remember that this scheduler asks the runner to remove the directory, so that the runner's report of the removal is authorized.
		if connection.is_some() {
			self.sandboxes
				.removing
				.insert(sandbox.clone(), runner.clone());
		}
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = match connection {
					// Ask the runner to remove the directory.
					Some(connection) => {
						let session = scheduler.server.session(&scheduler.server.context);
						let retry = tangram_futures::retry::Options {
							max_retries: 0,
							..scheduler.server.config.control.request_retry.clone().into()
						};
						let timeout = scheduler.server.config.control.request_timeout;
						let options = tangram_server_core::control::Options { retry, timeout };
						let arg = tg::runner::control::ServerRequestArg::DestroySandbox(
							tg::runner::control::DestroySandboxServerRequestArg {
								error: error.clone(),
								sandbox: sandbox.clone(),
							},
						);
						session
							.server
							.components
							.runner
							.send_runner_control_request(
								&session,
								&runner,
								&scheduler.id,
								connection,
								tangram_server_core::control::id(),
								arg,
								options,
							)
							.await
							.and_then(std::convert::identity)
							.and_then(|output| {
								output
									.try_unwrap_destroy_sandbox()
									.map_err(|_| tg::error!("expected a destroy sandbox response"))
									.map(|output| output.destroyed)
							})
					},

					// Wait for a runner that is not ready.
					None if waiting => {
						Err(tg::error!(%sandbox, %runner, "the sandbox's runner is unavailable"))
					},

					// Report that a runner that is declared dead did not remove the directory.
					None => Ok(false),
				};
				if result.is_err() {
					tokio::time::sleep(super::recovery::READ_RETRY_INTERVAL).await;
				}
				Operation::RemoveDestroyingSandbox {
					error,
					result,
					runner,
					sandbox,
				}
			}
			.boxed(),
		);
	}

	/// Finish the removal of a destroying sandbox's directory. A runner that removed the directory recorded the destruction, a runner that did not have the directory or that is declared dead never will, so this scheduler records it, and a removal that failed is tried again.
	pub(super) fn handle_remove_destroying_sandbox(
		&mut self,
		scheduler: &Scheduler,
		sandbox: tg::sandbox::Id,
		runner: tg::runner::Id,
		error: Option<tg::error::Data>,
		result: tg::Result<bool>,
	) {
		if result.is_ok() && self.sandboxes.removing.get(&sandbox) == Some(&runner) {
			self.sandboxes.removing.remove(&sandbox);
		}
		match result {
			Ok(true) => {},
			Ok(false) => {
				let scheduler = scheduler.clone();
				self.operations.push(
					async move {
						let result = scheduler
							.component
							.destroy_runner_sandbox(&scheduler.server, &sandbox, error)
							.await;
						Operation::Finished {
							context: "failed to record the destroyed sandbox",
							result,
						}
					}
					.boxed(),
				);
			},
			Err(error_) => {
				tracing::warn!(error = %error_.trace(), %sandbox, %runner, "failed to remove the destroying sandbox's directory");
				self.remove_destroying_sandbox(scheduler, sandbox, runner, error);
			},
		}
	}

	/// Read the destroying sandboxes that the index records on a runner that connected, so that this scheduler has the runner remove their directories, which it may not have finished before the runner or this scheduler restarted.
	pub(super) fn read_destroying_sandboxes(
		&mut self,
		scheduler: &Scheduler,
		runner: &tg::runner::Id,
	) {
		let scheduler = scheduler.clone();
		let runner = runner.clone();
		self.operations.push(
			async move {
				let result = async {
					let ids = scheduler.server.index.get_runner_sandboxes(&runner).await?;
					let indexed = scheduler.server.index.try_get_sandboxes(&ids).await?;
					let sandboxes = std::iter::zip(ids, indexed)
						.filter_map(|(id, indexed)| {
							let data = indexed?.data?.data;
							data.status
								.is_destroying()
								.then(|| (id, data.error.and_then(tg::Either::left)))
						})
						.collect::<Vec<_>>();
					Ok::<_, tg::Error>(sandboxes)
				}
				.await;
				let sandboxes = result.unwrap_or_else(|error| {
					tracing::error!(error = %error.trace(), %runner, "failed to read the runner's destroying sandboxes");
					Vec::new()
				});
				Operation::DestroyingSandboxes { runner, sandboxes }
			}
			.boxed(),
		);
	}

	/// Declare a runner dead whose connection expired or whose stream ended because it shut down, unless the request is for a connection that a newer connection of the runner replaced.
	pub(super) fn handle_remove_runner_request(
		&mut self,
		scheduler: &Scheduler,
		id: Option<String>,
		request: RemoveRunnerRequestArg,
	) {
		let current = self
			.runners
			.entries
			.get(&request.runner)
			.is_some_and(|runner| runner.connection == request.connection);
		if !current {
			if let Some(id) = id {
				let output = RemoveRunnerResponseOutput {
					runner: request.runner,
				};
				let response = super::request::Response::new(
					id,
					Ok(super::ResponseOutput::RemoveRunner(output)),
				);
				scheduler.send_response(response);
			}
			return;
		}
		tracing::info!(runner = %request.runner, connection = request.connection, "removing the runner");
		self.declare_runner_dead(scheduler, &request.runner, Removal::Removed { id });
	}

	/// Declare a deleted runner dead.
	pub(super) fn handle_delete_runner_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: &tangram_server_scheduler_core::DeleteRunnerRequestArg,
	) {
		tracing::info!(runner = %request.runner, "deleting the runner");
		self.declare_runner_dead(scheduler, &request.runner, Removal::Deleted { id });
	}

	/// Declare a runner dead. Forget the sandboxes that it accepted, end its unanswered offers, and discard the starts pinned to it, so that a boot that the runner's sandboxes are stopped for never starts later. This scheduler holds those boots, so their discard alone records their end, and the end of the runner's boots leaves them alone. Forget the ended boots of a deleted runner, which never connects again. Then, in an operation, end the runner's boots.
	fn declare_runner_dead(
		&mut self,
		scheduler: &Scheduler,
		runner: &tg::runner::Id,
		removal: Removal,
	) {
		let accepted = self.get_unreported_sandboxes(runner, &[]);
		let accepted = self.forget_accepted_sandboxes(scheduler, runner, &accepted);
		let deleted = matches!(removal, Removal::Deleted { .. });
		if let Some(entry) = self.remove_runner_entry(runner) {
			self.end_runner_offers(scheduler, runner, entry, deleted);
		}
		let mut queued = Vec::new();
		for discarded in self.discard_pinned_sandboxes(scheduler, runner) {
			queued.push(discarded.sandbox.clone());
			scheduler.publish_sandbox_discarded(self, discarded);
		}
		if deleted {
			self.sandboxes.ended.forget_runner(runner, &[]);
		}
		let scheduler = scheduler.clone();
		let runner = runner.clone();
		self.operations.push(
			async move {
				let result = scheduler
					.end_dead_runner(&runner, queued, accepted, deleted)
					.boxed()
					.await;
				match removal {
					Removal::Deleted { id } => Operation::DeleteRunner { id, result },
					Removal::Removed { id } => {
						let result = result.map(|()| RemoveRunnerResponseOutput { runner });
						Operation::RemoveRunner { id, result }
					},
				}
			}
			.boxed(),
		);
	}

	pub(super) fn handle_heartbeat(
		&mut self,
		notification: &HeartbeatNotification,
		now: tangram_server_core::clock::Instant,
	) -> bool {
		let Some(runner) = self.runners.entries.get_mut(&notification.runner) else {
			return false;
		};
		if runner.connection_failed
			|| runner.connection != notification.connection
			|| runner.heartbeat_index >= notification.heartbeat_index
		{
			return false;
		}
		// Forget potential capacity after a parent is no longer available for borrowing.
		let expired: Vec<_> = runner
			.borrowable
			.iter()
			.filter(|parent| {
				!notification.sandboxes.contains(*parent)
					&& self
						.parents
						.0
						.get(*parent)
						.and_then(|parent| parent.borrowable.as_ref())
						.is_some_and(|borrowable| {
							notification.heartbeat_index > borrowable.heartbeat_index
						})
			})
			.cloned()
			.collect();
		for parent_id in expired {
			runner.borrowable.remove(&parent_id);
			if let Some(parent) = self.parents.0.get_mut(&parent_id) {
				parent.borrowable = None;
				if parent.is_empty() {
					self.parents.0.remove(&parent_id);
				}
			}
		}
		runner.capacity = notification.capacity;
		runner.committed = tg::runner::Capacity::default();
		runner.heartbeat_at = now;
		runner.heartbeat_index = notification.heartbeat_index;
		self.queue.wake();

		true
	}

	/// Get the sandboxes whose current boot this scheduler holds, which are the ones that it queued, offered, or ended and holds until the index has recorded the end, the ones whose next boot a start is reserving, and the ones that it holds for recovery.
	pub(super) fn get_held_sandboxes(&self) -> Vec<tg::sandbox::Id> {
		let mut sandboxes = self
			.sandboxes
			.entries
			.iter()
			.filter(|(_, sandbox)| !matches!(sandbox.state, State::Accepted { .. }))
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		sandboxes.extend(self.sandboxes.reserving.keys().cloned());
		sandboxes.extend(self.get_recovering_sandboxes());

		sandboxes
	}

	/// Get the boots that a runner accepted, except the ones that it reports.
	pub(super) fn get_unreported_sandboxes(
		&self,
		id: &tg::runner::Id,
		reported: &[tg::runner::control::ArgSandbox],
	) -> Vec<(tg::sandbox::Id, u64)> {
		self.sandboxes
			.entries
			.iter()
			.filter(
				|(_, sandbox)| matches!(&sandbox.state, State::Accepted { runner } if runner.id == *id),
			)
			.map(|(sandbox, entry)| (sandbox.clone(), entry.boot))
			.filter(|(sandbox, boot)| {
				!reported
					.iter()
					.any(|reported| reported.id == *sandbox && reported.boot == *boot)
			})
			.collect()
	}

	/// Forget the boots that a runner accepted and no longer reports, if this scheduler still tracks them on the runner, remember them as ended, and return their sandboxes. The runner held a boot from its acceptance, so a boot that it no longer reports ended on the runner, which reports the end, or was lost with a runner that restarted or is declared dead, whose boots this scheduler ends.
	pub(super) fn forget_accepted_sandboxes(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::runner::Id,
		accepted: &[(tg::sandbox::Id, u64)],
	) -> Vec<tg::sandbox::Id> {
		let mut forgotten = Vec::new();
		for (sandbox, boot) in accepted {
			let tracked = self.sandboxes.entries.get(sandbox).is_some_and(|entry| {
				entry.boot == *boot
					&& matches!(&entry.state, State::Accepted { runner } if runner.id == *id)
			});
			if !tracked {
				continue;
			}
			self.remove_ended_sandbox(scheduler, sandbox);
			self.stop_serving_sandbox_holder(sandbox);
			forgotten.push(sandbox.clone());
		}

		forgotten
	}

	/// Cancel the boots and processes that a runner reported to this restarted scheduler and that the index does not have, which this scheduler lost before its first write of them. Forget each lost boot and ask the runner to destroy it, which ends the boot and finishes its processes as failed, and send each lost process a kill signal, which its runner finishes it for.
	pub(super) fn cancel_lost_sandboxes(
		&mut self,
		scheduler: &Scheduler,
		runner: &tg::runner::Id,
		connection: u64,
		lost: Lost,
	) {
		let Lost {
			processes,
			sandboxes,
		} = lost;

		// Forget and destroy the lost boots.
		for sandbox in sandboxes {
			tracing::warn!(sandbox = %sandbox.id, boot = sandbox.boot, %runner, "cancelling the boot that the index does not have");
			let accepted = self
				.sandboxes
				.entries
				.get(&sandbox.id)
				.is_some_and(|entry| {
					entry.boot == sandbox.boot
						&& matches!(&entry.state, State::Accepted { runner: accepted } if accepted.id == *runner)
				});
			if accepted {
				self.remove_ended_sandbox(scheduler, &sandbox.id);
				self.stop_serving_sandbox_holder(&sandbox.id);
			}
			let error = tg::error::Data {
				code: Some(tg::error::Code::Cancellation),
				message: Some("the scheduler lost the sandbox's record".to_owned()),
				..Default::default()
			};
			let destroy = super::sandbox::DestroySandbox {
				connection,
				error,
				request: None,
				runner: runner.clone(),
				sandbox: sandbox.id,
			};
			self.send_destroy_sandbox(scheduler, destroy);
		}

		// Kill the lost processes.
		for process in processes {
			tracing::warn!(%process, %runner, "cancelling the process that the index does not have");
			let scheduler = scheduler.clone();
			self.operations.push(
				async move {
					let session = scheduler.server.session(&scheduler.server.context);
					let id = tg::Either::Right(process);
					let arg = tg::process::signal::post::Arg {
						location: None,
						signal: tg::process::Signal::SIGKILL,
						tokens: tg::authorization::Tokens::default(),
					};
					let result = scheduler
						.server
						.components
						.process
						.try_signal_process(&session, &id, arg)
						.await
						.map(|_| ());
					Operation::Finished {
						context: "failed to cancel the lost process",
						result,
					}
				}
				.boxed(),
			);
		}
	}

	/// Record that the scheduler failed to record a runner's connection, and keep the runner, so that it takes no sandboxes and its expiry ends the sandboxes that it accepted unless it connects again.
	pub(super) fn fail_runner_connection(&mut self, id: &tg::runner::Id, connection: u64) {
		if let Some(runner) = self.runners.entries.get_mut(id)
			&& runner.connection == connection
		{
			runner.connection_failed = true;
		}
	}

	/// Remove a runner's entry and forget the capacity that its sandboxes lend, and return the entry, whose reservations are the capacity that each of its unanswered offers reserves.
	pub(super) fn remove_runner_entry(&mut self, id: &tg::runner::Id) -> Option<Runner> {
		let runner = self.runners.entries.remove(id)?;
		for parent_id in &runner.borrowable {
			let Some(parent) = self.parents.0.get_mut(parent_id) else {
				continue;
			};
			let remove = parent.borrowable.as_ref().is_some_and(|borrowable| {
				borrowable.runner == *id && borrowable.connection == Some(runner.connection)
			});
			if remove {
				parent.borrowable = None;
				if parent.is_empty() {
					self.parents.0.remove(parent_id);
				}
			}
		}

		Some(runner)
	}

	/// End the unanswered offers of a runner that is declared dead, and stop waiting for its report to this restarted scheduler, whose release ends the boots that the runner claimed. The sandboxes that the runner accepted are ended by the end of the runner's boots.
	fn end_runner_offers(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::runner::Id,
		runner: Runner,
		deleted: bool,
	) {
		self.forget_recovery_runner(scheduler, id);
		for sandbox in runner.reservations.into_keys() {
			self.end_unanswered_offer(scheduler, &sandbox, id, deleted);
		}
		self.queue.wake();
	}

	/// End the boot of a sandbox whose offer to a runner that this scheduler declared dead is unanswered, because the runner may have accepted the offer, so that the boot is never offered to a second runner while the first may hold it. End it after the boot's first record is written and indexed, remember the boot as ended once the end applies, unless the runner was deleted, so that this scheduler tells the runner when it connects again, and record the runner as the sandbox's runner, so that a later boot is offered only to it. Keep the sandbox's entry until the index has recorded the end, and destroy a sandbox that a dequeue waits for or whose ttl is zero.
	fn end_unanswered_offer(
		&mut self,
		scheduler: &Scheduler,
		sandbox: &tg::sandbox::Id,
		runner: &tg::runner::Id,
		deleted: bool,
	) {
		// Record the runner as the sandbox's runner, and decide whether the sandbox can stay stopped.
		let entry = self.sandboxes.entries.get_mut(sandbox).unwrap();
		let request = entry.request_mut();
		request.runner.get_or_insert_with(|| runner.clone());
		let zero = request
			.arg
			.time_to_live
			.is_some_and(|time_to_live| time_to_live.is_zero());
		let boot = entry.boot;
		let dequeue_requests = std::mem::take(&mut entry.dequeue_requests);
		let destroy = zero || !dequeue_requests.is_empty();
		let persist = entry.persist.clone();

		// End the boot.
		let error = tg::error!(%sandbox, %runner, "the sandbox's runner is unavailable")
			.to_data_or_id()
			.left()
			.unwrap_or_default();
		let status = if destroy {
			tg::sandbox::Status::Destroyed
		} else {
			tg::sandbox::Status::Stopped
		};
		let destroyed = {
			let scheduler = scheduler.clone();
			let error = error.clone();
			let runner = runner.clone();
			let sandbox = sandbox.clone();
			let task = (!deleted).then(|| scheduler.holder_sender.clone());
			async move {
				tangram_server_core::checkpoint!(scheduler.server, "scheduler.sandbox.end", %sandbox, status = %status)
					.await;
				let arg = EndDeadRunnerBootArg {
					boot,
					destroy,
					error,
					persist,
					runner: &runner,
					sandbox: &sandbox,
					task: task.as_ref(),
				};
				scheduler
					.component
					.end_dead_runner_boot(&scheduler.server, arg)
					.await
			}
			.boxed()
			.shared()
		};
		let write = destroyed.clone().map(|_| Ok(())).boxed();
		let request = self.sandboxes.entries[sandbox].request().clone();
		let error = tg::Either::Left(error);
		let write =
			self.end_sandbox_with_write(scheduler, sandbox, status, Some(error.clone()), write);

		// Reply to the waiting dequeues whether the write destroyed the sandbox, which a start that reserved the next boot first prevents, and tell the waiting callers of the end otherwise.
		if dequeue_requests.is_empty() {
			let discarded = super::sandbox::DiscardedSandbox {
				error,
				request,
				sandbox: sandbox.clone(),
				write,
			};
			scheduler.publish_sandbox_discarded(self, discarded);
		} else {
			let scheduler = scheduler.clone();
			self.operations.push(
				async move {
					let dequeued = destroyed.await;
					scheduler.send_dequeue_responses(dequeue_requests, dequeued);
					Operation::Finished {
						context: "failed to record the ended sandbox",
						result: Ok(()),
					}
				}
				.boxed(),
			);
		}
	}

	/// Return a sandbox whose offer the runner lost to the queue, and end it as destroyed if a dequeue waits for it.
	fn requeue_unanswered_sandbox(&mut self, scheduler: &Scheduler, sandbox: &tg::sandbox::Id) {
		self.requeue_sandbox(sandbox);
		let entry = self.sandboxes.entries.get_mut(sandbox).unwrap();
		let dequeue_requests = std::mem::take(&mut entry.dequeue_requests);
		if dequeue_requests.is_empty() {
			return;
		}
		self.end_sandbox_and_log(scheduler, sandbox, tg::sandbox::Status::Destroyed, None);
		scheduler.send_dequeue_responses(dequeue_requests, true);
	}

	/// Keep the reservations of a runner's unanswered offers on the runner's new entry.
	fn keep_unanswered_offers(
		&mut self,
		runner: &tg::runner::Id,
		offers: HashMap<tg::sandbox::Id, tg::runner::Capacity, tg::id::BuildHasher>,
	) {
		for (sandbox, reserved) in offers {
			let offered = self
				.sandboxes
				.entries
				.get(&sandbox)
				.is_some_and(|entry| matches!(entry.state, State::Offered { .. }));
			if !offered {
				continue;
			}
			let entry = self.runners.entries.get_mut(runner).unwrap();
			super::offer::add(&mut entry.reserved, reserved);
			entry.reservations.insert(sandbox, reserved);
		}
	}

	/// Get the unanswered offers to a runner that restarted, and the boots that it claimed in its reports to this restarted scheduler, for the runner's first connection to decide from the index.
	fn get_unanswered_offers(&self, runner: &tg::runner::Id) -> Vec<UnansweredOffer> {
		let mut unanswered = Vec::new();
		for sandbox in self.runners.entries[runner].reservations.keys() {
			let Some(entry) = self.sandboxes.entries.get(sandbox) else {
				continue;
			};
			let State::Offered { offer, .. } = &entry.state else {
				continue;
			};
			let offer = UnansweredOffer {
				boot: entry.boot,
				offer: Some(offer.clone()),
				sandbox: sandbox.clone(),
			};
			unanswered.push(offer);
		}
		unanswered.extend(self.get_claims(runner));

		unanswered
	}

	/// Act on the decisions of the unanswered offers to a runner that restarted, which its first connection made from the index. Return a boot that no runner wrote to the queue, because this scheduler still holds it, so that it is offered again. Forget a boot that the runner wrote, which the connection ended, because the runner had accepted it, and a boot that ended, which is never offered again, and remove it from this scheduler's saved queue. The boots that the runner claimed are decided with the runner's report.
	pub(super) fn handle_unanswered_offers(
		&mut self,
		scheduler: &Scheduler,
		runner: &tg::runner::Id,
		unanswered: Vec<UnansweredOffer>,
		decided: &BTreeMap<tg::sandbox::Id, super::recovery::SavedBoot>,
	) {
		for offer in unanswered {
			let Some(id) = &offer.offer else {
				continue;
			};

			// Skip an offer that is no longer the sandbox's current offer.
			let current = self.sandboxes.entries.get(&offer.sandbox).is_some_and(
				|entry| matches!(&entry.state, State::Offered { offer: current, .. } if current == id),
			);
			if !current {
				continue;
			}

			// Release the offer's reservation.
			if let Some(entry) = self.runners.entries.get_mut(runner)
				&& let Some(reserved) = entry.reservations.remove(&offer.sandbox)
			{
				super::offer::subtract(&mut entry.reserved, reserved);
			}

			// Return a boot that no runner wrote to the queue.
			let decision = decided.get(&offer.sandbox);
			if decision
				.is_none_or(|decision| matches!(decision, super::recovery::SavedBoot::Unwritten))
			{
				self.requeue_unanswered_sandbox(scheduler, &offer.sandbox);
				continue;
			}

			// Forget a boot that the runner wrote or that ended, remove a boot that ended from the saved queue, and reply to the waiting dequeues that the sandbox was not dequeued.
			let entry = self.sandboxes.entries.get_mut(&offer.sandbox).unwrap();
			let dequeue_requests = std::mem::take(&mut entry.dequeue_requests);
			let request = entry.request().clone();
			self.remove_queued_sandbox(&offer.sandbox);
			self.remove_sandbox(scheduler, &offer.sandbox);
			self.stop_serving_sandbox_holder(&offer.sandbox);
			if matches!(decision, Some(super::recovery::SavedBoot::Ended)) {
				self.delete_saved_boot(scheduler, request);
			}
			scheduler.send_dequeue_responses(dequeue_requests, false);
		}
		self.queue.wake();
	}
}

impl Scheduler {
	async fn add_runner(
		&self,
		request: AddRunnerRequestArg,
		connection: &Connection,
	) -> tg::Result<AddRunnerOutput> {
		let Connection {
			accepted,
			ended,
			number: connection,
			queued,
			recovering,
			unanswered,
			..
		} = connection;
		let (connection, recovering) = (*connection, *recovering);
		let accepted = accepted
			.iter()
			.map(|(sandbox, _)| sandbox.clone())
			.collect();
		let session = self.server.session(&self.server.context);
		let runner = self
			.server
			.components
			.runner
			.try_get_runner_data(&session, &request.runner)
			.await?;
		let owner = match runner {
			Some(runner) => runner.owner.and_then(|owner| owner.to_id()),
			None if self
				.server
				.config
				.roles
				.contains(&tangram_server_core::config::Role::Runner)
				&& self.server.config.runner.id.is_none()
				&& self.server.components.runner.state().id().as_ref() == Some(&request.runner) =>
			{
				None
			},
			None => {
				return Err(tg::error!(runner = %request.runner, "failed to find the runner"));
			},
		};

		// Record that the runner belongs to this scheduler, which the index rejects if the runner belongs to another.
		tangram_server_core::checkpoint!(self.server, "scheduler.runner.connect", runner = %request.runner, connection).await;
		self.server
			.components
			.runner
			.set_runner_scheduler(&session, &request.runner, &self.id)
			.await?;

		// Await indexing on the runner's first connection and on its first report to this restarted scheduler, because only the index shows whether the runner had accepted an unanswered offer, which boots the runner had on the index, and which reported boots and processes were lost. Fail the connection if indexing cannot be awaited while an offer to the runner is unanswered or the runner reports boots to this restarted scheduler, and read the index as it is otherwise.
		if (request.first || recovering)
			&& let Err(error) = self.index().await
		{
			if (request.first && !unanswered.is_empty())
				|| (recovering && !request.sandboxes.is_empty())
			{
				return Err(
					tg::error!(!error, runner = %request.runner, "failed to await indexing"),
				);
			}
			tracing::warn!(error = %error.trace(), "failed to await indexing");
		}

		// Get the reported boots and processes that the index does not have on the runner's first report to this restarted scheduler, which this scheduler lost before its first write of them, and which it cancels.
		let lost = if recovering {
			self.get_lost_sandboxes(&request.runner, &request.sandboxes)
				.await?
		} else {
			Lost::default()
		};

		// End the boots that the index records on a runner on its first connection after it starts, and that neither the runner reports nor this scheduler's queue has.
		let mut decided = BTreeMap::new();
		if request.first {
			// Decide the unanswered offers and the claimed boots from the index, and get the ones whose boots the runner wrote, which shows that the runner had accepted them, so that the end covers them.
			decided = self.decide_unanswered_offers(unanswered).await?;
			let written = decided
				.iter()
				.filter(|(_, decision)| decision.is_written())
				.map(|(sandbox, _)| sandbox.clone())
				.collect::<Vec<_>>();
			let mut queued = queued.clone();
			queued.retain(|sandbox| !written.contains(sandbox));

			// End the boots of the runner that restarted.
			let error = tg::error::Data {
				code: Some(tg::error::Code::Internal),
				message: Some("runner restarted".to_owned()),
				..Default::default()
			};
			let arg = EndRunnerSandboxesArg {
				accepted,
				checkpoint: "scheduler.runner.restarted",
				deleted: false,
				ended: None,
				error,
				held: request.sandboxes.clone(),
				queued,
				runner: request.runner.clone(),
				scheduler: Some(self.holder_sender.clone()),
				written,
			};
			self.component
				.end_runner_sandboxes(&self.server, &self.id, arg)
				.boxed()
				.await?;
		}

		tangram_server_core::checkpoint!(self.server, "scheduler.runner.added", runner = %request.runner).await;
		let output = AddRunnerResponseOutput {
			connection,
			ended: ended.clone(),
			runner: request.runner,
			scheduler: self.id.clone(),
		};
		let output = AddRunnerOutput {
			decided,
			lost,
			output,
			owner,
		};

		Ok(output)
	}

	/// Get the reported boots and processes that the index does not have. A boot that the runner started on the shortcut path, whose ID the runner proves that it derived, is not lost, because the runner's own server writes its first record.
	async fn get_lost_sandboxes(
		&self,
		runner: &tg::runner::Id,
		sandboxes: &[tg::runner::control::ArgSandbox],
	) -> tg::Result<Lost> {
		// Read the reported sandboxes.
		let ids = sandboxes
			.iter()
			.map(|sandbox| sandbox.id.clone())
			.collect::<Vec<_>>();
		let indexed = self
			.server
			.index
			.try_get_sandboxes(&ids)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the reported sandboxes"))?;

		// Get the lost boots, and the reported processes of the other boots that the index records at the reported boot.
		let mut lost = Lost::default();
		let mut processes = Vec::new();
		for (sandbox, indexed) in std::iter::zip(sandboxes, indexed) {
			let data = indexed
				.and_then(|indexed| indexed.data)
				.map(|data| data.data);
			let Some(data) = data else {
				let derived = sandbox.boot == 0
					&& sandbox
						.proof
						.as_ref()
						.is_some_and(|proof| proof.verify(&sandbox.id, runner).is_ok());
				if !derived {
					lost.sandboxes.push(sandbox.clone());
				}
				continue;
			};
			if data.boot == sandbox.boot && !data.status.is_stopped_or_destroyed() {
				processes.extend(sandbox.processes.iter().cloned());
			}
		}

		// Get the lost processes.
		let indexed = self
			.server
			.index
			.try_get_processes_with_data(&processes)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the reported processes"))?;
		lost.processes = std::iter::zip(processes, indexed)
			.filter(|(_, indexed)| indexed.as_ref().is_none_or(|(_, data)| data.is_none()))
			.map(|(process, _)| process)
			.collect();

		Ok(lost)
	}

	/// Decide the unanswered offers to a runner that restarted and the boots that it claimed from the index.
	async fn decide_unanswered_offers(
		&self,
		unanswered: &[UnansweredOffer],
	) -> tg::Result<BTreeMap<tg::sandbox::Id, super::recovery::SavedBoot>> {
		let ids = unanswered
			.iter()
			.map(|offer| offer.sandbox.clone())
			.collect::<Vec<_>>();
		let indexed = self
			.server
			.index
			.try_get_sandboxes(&ids)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the offered sandboxes"))?;
		let decided = decide_unanswered_offers(unanswered, indexed);

		Ok(decided)
	}

	/// End the boots of a runner that this scheduler declared dead, and destroy the sandboxes of a deleted runner, which never runs them again. The queued sandboxes are the ones whose current boot this scheduler holds, which the runner never held.
	pub(super) async fn end_dead_runner(
		&self,
		runner: &tg::runner::Id,
		queued: Vec<tg::sandbox::Id>,
		accepted: Vec<tg::sandbox::Id>,
		deleted: bool,
	) -> tg::Result<()> {
		// Await indexing, so that the runner's sandboxes are read with every write that was acknowledged before the runner was declared dead, and read the index as it is if indexing cannot be awaited.
		tangram_server_core::checkpoint!(self.server, "scheduler.runner.index", %runner).await;
		if let Err(error) = self.index().await {
			tracing::warn!(error = %error.trace(), "failed to await indexing");
		}

		// End the runner's boots.
		let (checkpoint, message) = if deleted {
			("scheduler.runner.deleted", "the runner was deleted")
		} else {
			("scheduler.runner.expired", "heartbeat expired")
		};
		let error = tg::error::Data {
			code: Some(tg::error::Code::Internal),
			message: Some(message.to_owned()),
			..Default::default()
		};
		let arg = EndRunnerSandboxesArg {
			accepted,
			checkpoint,
			deleted,
			ended: (!deleted).then(|| self.holder_sender.clone()),
			error,
			held: Vec::new(),
			queued,
			runner: runner.clone(),
			scheduler: Some(self.holder_sender.clone()),
			written: Vec::new(),
		};
		self.component
			.end_runner_sandboxes(&self.server, &self.id, arg)
			.boxed()
			.await?;
		if !deleted {
			tangram_server_core::checkpoint!(self.server, "scheduler.runner.removed", %runner)
				.await;
		}

		Ok(())
	}
}

impl Component {
	/// End the boots that the index records on a runner and that neither the runner reports nor the scheduler holds or has in its saved queue, each with the end numbered zero, which leaves the sandbox stopped and which the index applies only if the boot has not ended. Destroy a sandbox that cannot stay stopped.
	pub(super) async fn end_runner_sandboxes(
		&self,
		server: &Server,
		scheduler: &tg::scheduler::Id,
		arg: EndRunnerSandboxesArg,
	) -> tg::Result<()> {
		let EndRunnerSandboxesArg {
			accepted,
			checkpoint,
			deleted,
			ended,
			error,
			held,
			queued,
			runner,
			scheduler: task,
			written,
		} = arg;
		let runner = &runner;

		// Read the sandboxes that the index records on the runner, and add the ones whose boots the runner accepted.
		let mut sandboxes =
			server.index.get_runner_sandboxes(runner).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"),
			)?;
		for sandbox in accepted {
			if !sandboxes.contains(&sandbox) {
				sandboxes.push(sandbox);
			}
		}
		tracing::warn!(%runner, sandboxes = sandboxes.len(), reason = error.message.as_deref(), "ending the runner sandboxes");
		tangram_server_core::checkpoint!(server, checkpoint, %runner).await;
		let indexed =
			server.index.try_get_sandboxes(&sandboxes).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"),
			)?;

		// Get the scheduler's queue after reading the index, which is its saved queue, the sandboxes whose current boot it holds in memory, and the ones whose next boot a start is reserving, so that a boot that the index read shows in the queue is left to the scheduler, which holds it.
		let saved = self
			.get_scheduler_queue(server, scheduler, task.as_ref())
			.await?;
		for (sandbox, indexed) in std::iter::zip(sandboxes, indexed) {
			// Skip a sandbox in the scheduler's queue, unless the runner wrote the boot of its unanswered offer.
			let in_queue = queued.contains(&sandbox) || saved.contains(&sandbox);
			if in_queue && !written.contains(&sandbox) {
				continue;
			}

			// Skip a sandbox whose recorded boot the runner holds.
			let data = indexed
				.and_then(|indexed| indexed.data)
				.map(|data| data.data);
			let boot = data.as_ref().map(|data| data.boot);
			if held
				.iter()
				.any(|held| held.id == sandbox && Some(held.boot) == boot)
			{
				continue;
			}

			// Destroy a sandbox with a zero ttl, because its processes are lost, and a sandbox of a deleted runner, because it never starts again, after its boot ends.
			let time_to_live = data.and_then(|data| data.time_to_live);
			let zero = time_to_live.is_some_and(|time_to_live| time_to_live.is_zero());
			let destroy = deleted || zero;

			// End the boot, and destroy a sandbox that cannot stay stopped.
			let Some(boot) = boot else {
				continue;
			};
			let arg = EndDeadRunnerBootArg {
				boot,
				destroy,
				error: error.clone(),
				persist: None,
				runner,
				sandbox: &sandbox,
				task: ended.as_ref(),
			};
			self.end_dead_runner_boot(server, arg).boxed().await;
		}

		Ok(())
	}

	/// End a boot of a runner that this scheduler declared dead, which every end of a dead runner's boot by this scheduler goes through. Wait until the boot's first record is written and indexed, so that the end never reads the index before the record that it ends. Write the end numbered zero, which leaves the sandbox stopped, records the runner as the sandbox's runner, and removes the boot from the scheduler's queue, and which the index applies only if the boot has not ended, failing the boot's unfinished processes in the same transaction. Then destroy a sandbox that cannot stay stopped with the destroy of a stopped sandbox, which the index applies only while the sandbox is stopped at the boot or is destroying, so that a start that reserved the next boot wins. Write each again until the index answers it, because the scheduler may remember the boot as ended and its runner then gives it up. Tell the scheduling task of the boot once the end applied, so that it remembers the boot as ended. Return whether the destroy applied.
	pub(super) async fn end_dead_runner_boot(
		&self,
		server: &Server,
		arg: EndDeadRunnerBootArg<'_>,
	) -> bool {
		let EndDeadRunnerBootArg {
			boot,
			destroy,
			error,
			persist,
			runner,
			sandbox,
			task,
		} = arg;

		// Wait until the boot's first record is written and indexed.
		if let Some(persist) = persist {
			persist.await.ok();
			while let Err(error) = super::record::index(server).await {
				tracing::error!(error = %error.trace(), %sandbox, boot, "failed to await indexing before the end of the boot");
				tokio::time::sleep(super::recovery::READ_RETRY_INTERVAL).await;
			}
		}

		// End the boot.
		let ended = loop {
			let result = self
				.end_runner_sandbox(
					server,
					sandbox,
					Some(boot),
					Some(runner),
					Some(error.clone()),
				)
				.boxed()
				.await;
			match result {
				Ok(ended) => break ended,
				Err(error) => {
					tracing::error!(error = %error.trace(), %sandbox, boot, %runner, "failed to end the boot of the dead runner");
					tokio::time::sleep(super::recovery::READ_RETRY_INTERVAL).await;
				},
			}
		};

		// Remember the boot as ended once the end applied.
		if ended && let Some(task) = task {
			let message = super::HolderMessage::Ended {
				boot,
				runner: runner.clone(),
				sandbox: sandbox.clone(),
			};
			task.send(message).ok();
		}

		// Destroy a sandbox that cannot stay stopped, and return whether the write destroyed it.
		if !destroy {
			return false;
		}
		loop {
			let result = self
				.put_runner_sandbox_status(
					server,
					sandbox,
					Some(boot),
					None,
					Some(error.clone()),
					tg::sandbox::Status::Destroyed,
				)
				.boxed()
				.await;
			match result {
				Ok(destroyed) => break destroyed.is_some(),
				Err(error) => {
					tracing::error!(error = %error.trace(), %sandbox, boot, %runner, "failed to destroy the sandbox of the dead runner");
					tokio::time::sleep(super::recovery::READ_RETRY_INTERVAL).await;
				},
			}
		}
	}

	/// Get the sandboxes in a scheduler's queue, which are the ones in its saved queue and, if its scheduling task runs, the ones whose current boot it holds in memory, including the ones whose next boot a start is reserving.
	async fn get_scheduler_queue(
		&self,
		server: &Server,
		scheduler: &tg::scheduler::Id,
		task: Option<&tokio::sync::mpsc::UnboundedSender<super::HolderMessage>>,
	) -> tg::Result<HashSet<tg::sandbox::Id, tg::id::BuildHasher>> {
		// Read the saved queue.
		let mut queue = server
			.index
			.get_scheduler_sandboxes(scheduler)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the scheduler's sandboxes"))?
			.into_iter()
			.map(|sandbox| sandbox.id)
			.collect::<HashSet<_, tg::id::BuildHasher>>();

		// Ask the scheduling task for the sandboxes that it holds in memory.
		if let Some(task) = task {
			let (sender, receiver) = tokio::sync::oneshot::channel();
			task.send(super::HolderMessage::Held { sender })
				.map_err(|_| tg::error!("the scheduling task stopped"))?;
			let held = receiver
				.await
				.map_err(|_| tg::error!("the scheduling task stopped"))?;
			queue.extend(held);
		}

		Ok(queue)
	}

	/// Destroy a sandbox of a runner, which the index applies only while the sandbox is stopped at its boot, because a stopped sandbox has no holder, or while it is destroying. End a running boot first.
	pub(crate) async fn destroy_runner_sandbox(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		error: Option<tg::error::Data>,
	) -> tg::Result<()> {
		self.end_runner_sandbox(server, id, None, None, error.clone())
			.boxed()
			.await?;
		self.put_runner_sandbox_status(
			server,
			id,
			None,
			None,
			error,
			tg::sandbox::Status::Destroyed,
		)
		.boxed()
		.await?;

		Ok(())
	}

	/// End the running boot of a runner's sandbox with the end numbered zero, which leaves the sandbox stopped, and which the index applies only if the boot has not ended, failing the boot's unfinished processes in the same transaction. Record the runner, if one is given, as the sandbox's runner, because the runner may hold the boot without having written it. Leave a sandbox that moved past the boot that the caller read. Return whether the end applied.
	pub(crate) async fn end_runner_sandbox(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		boot: Option<u64>,
		runner: Option<&tg::runner::Id>,
		error: Option<tg::error::Data>,
	) -> tg::Result<bool> {
		let ended = self
			.put_runner_sandbox_status(
				server,
				id,
				boot,
				runner,
				error,
				tg::sandbox::Status::Stopped,
			)
			.boxed()
			.await?;
		if let Some(boot) = ended {
			self.finish_ended_boot_processes(server, id, boot)
				.boxed()
				.await;
		}

		Ok(ended.is_some())
	}

	/// Write the end of a runner's sandbox's boot numbered zero. A stop ends a running boot. A destroy applies only while the sandbox is stopped at its boot, because a stopped sandbox has no holder and the index decides its destroy, or while it is destroying. Return the boot that the write ended, if the index applied it.
	async fn put_runner_sandbox_status(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		read_boot: Option<u64>,
		runner: Option<&tg::runner::Id>,
		error: Option<tg::error::Data>,
		status: tg::sandbox::Status,
	) -> tg::Result<Option<u64>> {
		// Read the sandbox.
		let now = server.clock.unix_timestamp();
		let Some(mut indexed) = server.index.try_get_sandbox(id).await.map_err(
			|source| tg::error!(!source, %id, "failed to get the sandbox from the index"),
		)?
		else {
			return Ok(None);
		};
		let Some(data) = indexed.data.as_mut() else {
			return Ok(None);
		};

		// Leave a sandbox that moved past the boot that the caller read, a boot that already ended for a stop, and a destroyed sandbox.
		let boot = data.data.boot;
		if read_boot.is_some_and(|read_boot| boot > read_boot) {
			return Ok(None);
		}
		let applies = match status {
			tg::sandbox::Status::Destroyed => {
				data.data.status.is_stopped() || data.data.status.is_destroying()
			},
			tg::sandbox::Status::Destroying
			| tg::sandbox::Status::Started
			| tg::sandbox::Status::Starting
			| tg::sandbox::Status::Stopped
			| tg::sandbox::Status::Stopping => {
				!data.data.status.is_stopped_or_destroyed() && !data.data.status.is_destroying()
			},
		};
		if !applies {
			return Ok(None);
		}
		tracing::warn!(sandbox = %id, reason = error.as_ref().and_then(|error| error.message.as_deref()), %status, "ending the runner sandbox");
		let stopped_boot = data.data.status.is_stopped().then_some(boot);

		// Set the sandbox's error and status, and clear its usage, which the index already records for each boot, so that the sum of every boot is not recorded again as this boot's usage. Record no error if the sandbox ends without one, except that a destroying or stopped sandbox keeps the error that it recorded.
		if error.is_some() || !(data.data.status.is_destroying() || data.data.status.is_stopped()) {
			data.data.error = error.map(tg::Either::Left);
		}
		data.data.status = status;
		data.data.usage = None;

		// Record a stop as of now, so that the stopped sandbox is charged and expires from now rather than from an earlier stop.
		if status.is_stopped() {
			data.data.expires_at = data.data.time_to_live.map(|time_to_live| {
				now.saturating_add(time_to_live.as_secs().try_into().unwrap_or(i64::MAX))
			});
			data.data.stopped_at = Some(now);
		}
		let account = if server.config.usage.enabled {
			indexed.account
		} else {
			None
		};

		// Remove the ended boot from its scheduler's queue in the same write as its status, because no scheduler holds an ended boot.
		let delete = tangram_index::scheduler::sandbox::delete::Arg {
			boot,
			sandbox: id.clone(),
		};
		let put = tangram_index::sandbox::put::Arg {
			account,
			created_at: indexed.created_at,
			data: indexed.data,
			id: id.clone(),
			location: None,
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			process: None,
			processes: None,
			runner: indexed.runner.or_else(|| runner.cloned()),
			scheduler: None,
			sequence: 0,
			stopped_boot,
			stopped_usage: None,
			touched_at: now,
		};
		let items = vec![
			tangram_index::Item::PutSandbox(Box::new(put)),
			tangram_index::Item::DeleteSchedulerSandbox(Box::new(delete)),
		];
		let arg = tangram_index::write::Arg { items };
		let applied = server
			.index
			.write(arg)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to update the ended sandbox in the index"),
			)?
			.is_ok();
		if applied {
			server
				.components
				.sandbox
				.spawn_publish_sandbox_status_task(server, id);
		}
		let output = applied.then_some(boot);

		Ok(output)
	}

	/// Publish the statuses and end the logs of the processes of a boot that this scheduler ended, which the index failed in the transaction of the end. Doing this again is harmless, so a restarted scheduler does it again for each boot that it finds ended this way.
	pub(super) async fn finish_ended_boot_processes(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		boot: u64,
	) {
		let result = async {
			// Read the sandbox's processes of the boot.
			let length = server
				.index
				.try_get_sandbox_processes_count(id)
				.await?
				.unwrap_or_default();
			let processes = server
				.index
				.try_get_sandbox_processes(id, std::io::SeekFrom::Start(0), length)
				.await?
				.unwrap_or_default();
			let indexed = server.index.try_get_processes_with_data(&processes).await?;

			// End the log of each finished process of the boot without a stored log, and publish its status.
			for (process, indexed) in std::iter::zip(processes, indexed) {
				let Some((indexed, Some(data))) = indexed else {
					continue;
				};
				if indexed.boot != Some(boot) || !data.status.is_finished() {
					continue;
				}
				if data.log.is_none()
					&& data.started_at.is_some()
					&& let Err(error) = self.end_ended_process_log(server, &process).await
				{
					tracing::error!(error = %error.trace(), %process, "failed to end the process log");
				}
				server
					.components
					.process
					.notify_process_log(server, &process);
				server
					.components
					.process
					.spawn_publish_process_status_task(server, &process);
			}
			Ok::<_, tg::Error>(())
		}
		.await;
		if let Err(error) = result {
			tracing::error!(error = %error.trace(), sandbox = %id, boot, "failed to finish the processes of the ended boot");
		}
	}

	/// Store the end of the log of a process that the index failed, at the positions of its stored chunks, because its runner cannot send the end.
	async fn end_ended_process_log(
		&self,
		server: &Server,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		if server.cache.try_get_log_end(process).await?.is_some() {
			return Ok(());
		}

		// Get the stored positions.
		let length = async |streams| {
			let arg = log::length::Arg {
				process: process.clone(),
				streams,
			};
			let position = server
				.cache
				.try_get_log_length(arg)
				.await?
				.unwrap_or_default();
			Ok::<_, tg::Error>(position)
		};
		let (position, stderr_position, stdout_position) = tokio::try_join!(
			length(BTreeSet::from([
				tg::process::stdio::Stream::Stderr,
				tg::process::stdio::Stream::Stdout,
			])),
			length(BTreeSet::from([tg::process::stdio::Stream::Stderr])),
			length(BTreeSet::from([tg::process::stdio::Stream::Stdout])),
		)
		.map_err(|error| tg::error!(!error, %process, "failed to get the log positions"))?;
		let end = tg::process::log::End {
			position,
			stderr_position,
			stdout_position,
		};
		let arg = log::end::Arg {
			end,
			process: process.clone(),
		};

		// Store the end.
		server
			.cache
			.put_log_end(arg)
			.await
			.map_err(|error| tg::error!(!error, %process, "failed to store the log end"))?;

		Ok(())
	}
}
