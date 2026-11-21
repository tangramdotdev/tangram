use {
	super::{
		Operation, ResponseOutput, Scheduler, SchedulerState, placement::Placement, sandbox::State,
	},
	futures::FutureExt as _,
	std::collections::{BTreeSet, HashMap},
	tangram_client::prelude::*,
	tangram_server_scheduler_core::{
		FinishRunnerProcessesRequestArg, FinishRunnerProcessesResponseOutput,
	},
};

type Key = (tg::sandbox::Id, u64);

/// The boots that runners held and that ended or that a later boot replaced, which this scheduler remembers so that it can authorize a runner's late report of one, until the runner restarts or is removed, or for between one and two of the runner ttls.
#[derive(Default)]
pub(super) struct Ended {
	current: HashMap<Key, EndedBoot>,
	previous: HashMap<Key, EndedBoot>,
	rotated_at: Option<tangram_server_core::clock::Instant>,
}

/// A boot that a runner held and that ended or that a later boot replaced.
pub(super) struct EndedBoot {
	/// The processes that this scheduler offered into the boot.
	pub(super) processes: BTreeSet<tg::process::Id>,

	/// The runner that this scheduler gave the boot to, or that derived the sandbox's ID.
	pub(super) runner: tg::runner::Id,
}

/// A runner's report about a boot, which this scheduler authorizes before it acts on any of it.
pub(super) struct Report<'a> {
	pub(super) boot: u64,

	/// The processes that the report names.
	pub(super) processes: Vec<&'a tg::process::Id>,

	/// The proof that the runner derived the sandbox's ID, for a sandbox that the runner started on the shortcut path.
	pub(super) proof: Option<&'a tg::sandbox::id::Proof>,

	pub(super) runner: &'a tg::runner::Id,

	pub(super) sandbox: &'a tg::sandbox::Id,
}

/// This scheduler's decision on a runner's report.
pub(super) enum Authorization {
	/// The runner holds or held the boot, so this scheduler acts on the report.
	Authorized(Authorized),

	/// This scheduler cannot authorize the report yet, such as while it recovers after a restart, so the runner sends it again.
	NotReady,

	/// This scheduler refuses the report, so it acts on nothing in it, and the runner does not send it again.
	Refused(tg::Error),
}

pub(super) struct Authorized {
	pub(super) held: Held,

	/// The processes that the report names and that this scheduler offered into the boot, which are the only ones that the report may act on.
	pub(super) processes: BTreeSet<tg::process::Id>,
}

/// How this scheduler knows that the runner holds or held the boot.
pub(super) enum Held {
	/// This scheduler gave the boot to the runner.
	Accepted,

	/// The runner derived the sandbox's ID and started the boot on the shortcut path, and this scheduler does not know the boot.
	Derived,

	/// The runner held the boot, which ended or which a later boot replaced.
	Ended,

	/// This scheduler offered the boot to the runner, whose answer is unanswered.
	Offered(Placement),
}

impl Ended {
	/// Remember a boot that a runner held and that ended or that a later boot replaced.
	pub(super) fn insert(&mut self, sandbox: tg::sandbox::Id, boot: u64, ended: EndedBoot) {
		self.current.insert((sandbox, boot), ended);
	}

	pub(super) fn get(&self, sandbox: &tg::sandbox::Id, boot: u64) -> Option<&EndedBoot> {
		let key = (sandbox.clone(), boot);
		self.current.get(&key).or_else(|| self.previous.get(&key))
	}

	/// Forget the boots of a runner that restarted or that was removed, which sends no more reports of them.
	pub(super) fn forget_runner(&mut self, runner: &tg::runner::Id) {
		self.current.retain(|_, ended| ended.runner != *runner);
		self.previous.retain(|_, ended| ended.runner != *runner);
	}

	/// Forget the boots that ended more than a runner ttl before the last rotation, because a runner that does not report within its ttl is removed.
	pub(super) fn rotate(
		&mut self,
		now: tangram_server_core::clock::Instant,
		time_to_live: std::time::Duration,
	) {
		let Some(rotated_at) = self.rotated_at else {
			self.rotated_at = Some(now);
			return;
		};
		if now - rotated_at > time_to_live {
			self.previous = std::mem::take(&mut self.current);
			self.rotated_at = Some(now);
		}
	}
}

impl SchedulerState {
	/// Authorize a runner's report about a boot, which every report from a runner that finishes, ends, or records something goes through before this scheduler acts on any of it. The runner holds or held the boot if this scheduler gave or offered the boot to it, if this scheduler remembers that the runner held the boot that ended, or if the runner proves that it derived the sandbox's ID for the first boot of a sandbox that it started on the shortcut path. A report that this scheduler cannot authorize is refused, unless the scheduler is recovering after a restart and may know the boot once its runners have reported. Of the processes that the report names, only the ones that this scheduler offered into the boot are authorized.
	pub(super) fn authorize_runner_report(&self, report: &Report) -> Authorization {
		let Report {
			boot,
			processes,
			proof,
			runner,
			sandbox,
		} = report;
		let boot = *boot;

		// Find the boot that this scheduler gave or offered to the runner.
		let entry = self
			.sandboxes
			.entries
			.get(*sandbox)
			.filter(|entry| entry.boot == boot);
		let held = entry.and_then(|entry| match &entry.state {
			State::Accepted { runner: accepted } if accepted.id == **runner => {
				Some((Held::Accepted, &entry.processes))
			},
			State::Offered { placement, .. } if placement.runner().id == **runner => {
				Some((Held::Offered(placement.clone()), &entry.processes))
			},
			State::Accepted { .. }
			| State::Offered { .. }
			| State::Queued
			| State::Terminal { .. } => None,
		});

		// Find the boot that the runner held and that ended.
		let held = held.or_else(|| {
			self.sandboxes
				.ended
				.get(sandbox, boot)
				.filter(|ended| ended.runner == **runner)
				.map(|ended| (Held::Ended, &ended.processes))
		});

		// Verify the proof that the runner derived the sandbox's ID.
		let none = BTreeSet::new();
		let held = held.or_else(|| {
			let derived =
				boot == 0 && proof.is_some_and(|proof| proof.verify(sandbox, runner).is_ok());
			derived.then_some((Held::Derived, &none))
		});

		// Refuse a report that this scheduler cannot authorize, or ask for it again while this scheduler recovers.
		let Some((held, offered)) = held else {
			if self.recovery.is_some() {
				return Authorization::NotReady;
			}
			let error = tg::error!(%sandbox, %boot, %runner, "the runner does not hold the boot");
			return Authorization::Refused(error);
		};

		// Authorize the processes that this scheduler offered into the boot.
		let mut authorized = BTreeSet::new();
		for process in processes {
			if offered.contains(*process) {
				authorized.insert((*process).clone());
			} else {
				tracing::debug!(%process, %sandbox, %boot, %runner, "the scheduler did not offer the reported process into the boot");
			}
		}
		let authorized = Authorized {
			held,
			processes: authorized,
		};

		Authorization::Authorized(authorized)
	}

	/// Handle a runner's report of processes that it accepted for a boot and that cannot run. Authorize the report, and then, in an operation, finish as failed the reported processes that this scheduler offered into the boot.
	pub(super) fn handle_finish_runner_processes_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: FinishRunnerProcessesRequestArg,
	) {
		let FinishRunnerProcessesRequestArg { finish, runner } = request;

		// Authorize the report.
		let processes = finish
			.processes
			.iter()
			.filter_map(|process| process.id.as_ref())
			.collect();
		let report = Report {
			boot: finish.boot,
			processes,
			proof: None,
			runner: &runner,
			sandbox: &finish.sandbox,
		};
		let authorized = match self.authorize_runner_report(&report) {
			Authorization::Authorized(authorized) => authorized,
			Authorization::NotReady => {
				let output = FinishRunnerProcessesResponseOutput {
					retry: Some(tg::control::Retry::NotReady),
				};
				let output = ResponseOutput::FinishRunnerProcesses(output);
				let response = super::request::Response::new(id, Ok(output));
				scheduler.send_response(self, response);
				return;
			},
			Authorization::Refused(error) => {
				let response = super::request::Response::new(id, Err(error));
				scheduler.send_response(self, response);
				return;
			},
		};

		// Finish the authorized processes in an operation.
		let processes = finish
			.processes
			.into_iter()
			.filter(|process| {
				process
					.id
					.as_ref()
					.is_some_and(|id| authorized.processes.contains(id))
			})
			.collect::<Vec<_>>();
		let error = finish.error;
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				scheduler.finish_reported_processes(processes, error).await;
				let output = FinishRunnerProcessesResponseOutput { retry: None };
				Operation::FinishRunnerProcesses {
					id,
					result: Ok(output),
				}
			}
			.boxed(),
		);
	}
}

impl Scheduler {
	/// Finish as failed with the error the processes of an authorized report, which this scheduler offered into the reported boot, from the runner's report of its memory.
	pub(super) async fn finish_reported_processes(
		&self,
		processes: Vec<tg::runner::control::Process>,
		error: tg::Either<tg::error::Data, tg::error::Id>,
	) {
		for process in processes {
			let Some(id) = &process.id else {
				continue;
			};
			let data = process.data.without_location_and_tokens_except_sync();
			let result = self
				.server
				.components
				.process
				.finish_queued_process(&self.server, id, data, error.clone())
				.await;
			if let Err(error) = result {
				tracing::warn!(error = %error.trace(), process = %id, "failed to finish the reported process");
			}
		}
	}
}
