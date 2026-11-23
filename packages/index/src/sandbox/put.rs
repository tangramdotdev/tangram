use {
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
	tangram_util::serde::is_default,
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub account: Option<crate::usage::Account>,

	#[tangram_serialize(id = 1)]
	pub created_at: i64,

	#[tangram_serialize(id = 2)]
	pub data: Option<tg::sandbox::get::Output>,

	#[tangram_serialize(id = 3)]
	pub id: tg::sandbox::Id,

	/// The sandbox location, or `None` to preserve the indexed location.
	#[tangram_serialize(id = 6)]
	pub location: Option<tg::Location>,

	/// Permissions granted after validation. A put by root or the sandbox itself grants them even if its data loses, so that permissions merge as a union, and another principal's put grants them only if the sandbox is new or both `data` and `processes` are `Some`.
	#[tangram_serialize(id = 8)]
	pub permissions: Vec<crate::permission::put::Arg>,

	#[tangram_serialize(id = 9)]
	pub principal: tg::Principal,

	/// The process that the sandbox is created with, which is put after the sandbox, in the same transaction.
	#[tangram_serialize(default, id = 13, skip_serializing_if = "Option::is_none")]
	pub process: Option<Box<crate::process::put::Arg>>,

	#[tangram_serialize(id = 7)]
	pub processes: Option<Vec<tg::process::Id>>,

	#[tangram_serialize(id = 4)]
	pub runner: Option<tg::runner::Id>,

	/// The scheduler that queues the sandbox, which the record keeps from the first put that names one.
	#[tangram_serialize(default, id = 17, skip_serializing_if = "Option::is_none")]
	pub scheduler: Option<tg::scheduler::Id>,

	/// The position of the write among its holder's writes of the boot, which starts at zero for each boot, so that the index applies a holder's lagging writes in order.
	#[tangram_serialize(default, id = 15, skip_serializing_if = "is_default")]
	pub sequence: u64,

	/// The boot that the sandbox must be stopped at when this put is applied, which makes the put a conditional change of a stopped sandbox, such as a start that reserves the next boot or a destroy, so that of several such changes only the first is applied.
	#[tangram_serialize(default, id = 14, skip_serializing_if = "Option::is_none")]
	pub stopped_boot: Option<u64>,

	/// The storage usage of a boot's stopped directory so far, which only grows.
	#[tangram_serialize(default, id = 10, skip_serializing_if = "Option::is_none")]
	pub stopped_usage: Option<StoppedUsage>,

	#[tangram_serialize(id = 5)]
	pub touched_at: i64,
}

/// The record that a put of a sandbox produces.
pub struct Merge {
	/// Whether the index puts the arg's permissions.
	pub permissions: bool,

	pub sandbox: super::Sandbox,

	/// The boot whose end this put recorded with the number zero, which is the end that the sandbox's scheduler writes for a dead runner, so that the index fails the boot's unfinished processes in the same transaction.
	pub zero_end: Option<u64>,
}

/// The merged sandbox data and per-boot usage that a put records.
struct DataMerge {
	data: Option<tg::sandbox::get::Output>,

	/// The sequence of the write that the merged data is from.
	sequence: u64,

	stopped_usages: BTreeMap<u64, tg::sandbox::Usage>,

	/// Whether the arg's data superseded the existing data, so that the record takes the arg's account, location, and runner.
	superseded: bool,

	usages: BTreeMap<u64, tg::sandbox::Usage>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct StoppedUsage {
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub usage: tg::sandbox::Usage,
}

impl Arg {
	pub fn validate(&self) -> tg::Result<()> {
		if !self.principal.is_root()
			&& self.principal != tg::Principal::Sandbox(self.id.clone())
			&& self
				.data
				.as_ref()
				.is_some_and(|data| !data.data.status.is_destroyed())
		{
			return Err(tg::error!("expected a destroyed sandbox"));
		}
		let Some(processes) = &self.processes else {
			return Ok(());
		};
		let mut ids = BTreeSet::new();
		for process in processes {
			if !ids.insert(process) {
				return Err(tg::error!("the sandbox processes must be unique"));
			}
		}
		Ok(())
	}

	/// Reject a put that reserves the next boot unless the sandbox is stopped at the boot that the put names.
	pub fn validate_stopped_boot(&self, existing: Option<&super::Sandbox>) -> tg::Result<()> {
		let Some(boot) = self.stopped_boot else {
			return Ok(());
		};
		let stopped = existing
			.and_then(|sandbox| sandbox.data.as_ref())
			.is_some_and(|data| data.data.status.is_stopped() && data.data.boot == boot);
		if !stopped {
			return Err(tg::error!(id = %self.id, %boot, "the sandbox is not stopped at the boot"));
		}
		Ok(())
	}

	/// Whether the sandbox's record is what this put of a stopped sandbox produces, with the put's status, boot, and runner.
	#[must_use]
	pub fn is_applied(&self, existing: Option<&super::Sandbox>) -> bool {
		let (Some(data), Some(existing)) = (&self.data, existing) else {
			return false;
		};
		existing.data.as_ref().is_some_and(|existing| {
			existing.data.status == data.data.status && existing.data.boot == data.data.boot
		}) && existing.runner == self.runner
	}

	pub fn validate_existing(&self, existing: &super::Sandbox) -> tg::Result<tg::Result<()>> {
		if self.principal.is_root() || self.principal == tg::Principal::Sandbox(self.id.clone()) {
			return Ok(Ok(()));
		}
		if self.created_at != existing.created_at {
			return Ok(Err(tg::error!("cannot replace an existing sandbox")));
		}
		if let Some(data) = &self.data {
			let Some(existing) = &existing.data else {
				return Ok(Err(tg::error!("cannot verify the existing sandbox data")));
			};
			let data = serde_json::to_value(&data.data)
				.map_err(|error| tg::error!(!error, "failed to serialize the sandbox contents"))?;
			let existing = serde_json::to_value(&existing.data)
				.map_err(|error| tg::error!(!error, "failed to serialize the sandbox contents"))?;
			if data != existing {
				return Ok(Err(tg::error!("cannot replace an existing sandbox")));
			}
		}
		Ok(Ok(()))
	}

	/// Whether the merge compares the arg's processes with the existing record's processes, which the index reads for it.
	#[must_use]
	pub fn compares_processes(&self, existing: Option<&super::Sandbox>) -> bool {
		existing.is_some_and(|existing| existing.set.processes)
			&& !self.principal.is_root()
			&& self.principal != tg::Principal::Sandbox(self.id.clone())
			&& self.processes.is_some()
	}

	/// Merge the put with the existing record, which is the one rule that every index backend applies, so that writes applied in any order produce the same record. The existing processes are the first processes of the existing record, which the index reads when `compares_processes` is true.
	pub fn merge(
		&self,
		existing: Option<&super::Sandbox>,
		existing_processes: Option<&[tg::process::Id]>,
	) -> tg::Result<tg::Result<Merge>> {
		// Validate the arg.
		if let Err(error) = self.validate() {
			return Ok(Err(error));
		}

		// Reject a reservation of the next boot unless the sandbox is stopped at the boot that the put names.
		if let Err(error) = self.validate_stopped_boot(existing) {
			return Ok(Err(error));
		}

		// Validate the sandbox data and processes against the existing record.
		if let Some(existing) = existing {
			if let Err(error) = self.validate_existing(existing)? {
				return Ok(Err(error));
			}
			if !self.principal.is_root()
				&& self.principal != tg::Principal::Sandbox(self.id.clone())
				&& let Some(processes) = &self.processes
			{
				if !existing.set.processes {
					return Ok(Err(tg::error!(
						"cannot verify the existing sandbox processes"
					)));
				}
				if processes.as_slice() != existing_processes.unwrap_or_default() {
					return Ok(Err(tg::error!(
						"cannot replace the existing sandbox processes"
					)));
				}
			}
		}

		// Put the permissions of every accepted put of root or the sandbox itself, even one whose data loses, so that permissions merge as a union. Another principal's put proves what it grants only if the sandbox is new or the put carries both its data and its processes.
		let permissions = self.principal.is_root()
			|| self.principal == tg::Principal::Sandbox(self.id.clone())
			|| existing.is_none()
			|| (self.data.is_some() && self.processes.is_some());

		// Keep the existing data, account, and runner for another principal's put.
		let mut arg = std::borrow::Cow::Borrowed(self);
		if let Some(existing) = existing
			&& !arg.principal.is_root()
			&& arg.principal != tg::Principal::Sandbox(arg.id.clone())
		{
			let arg = arg.to_mut();
			arg.account.clone_from(&existing.account);
			arg.data = None;
			arg.runner.clone_from(&existing.runner);
		}
		let arg = arg.as_ref();

		// Keep the data that is latest in the sandbox lifecycle, because writes may be applied out of order.
		let DataMerge {
			mut data,
			sequence,
			stopped_usages,
			superseded,
			usages,
		} = arg.merge_data(existing);

		// Take the account, the location, and the runner with the data that wins, and keep the other's location and runner where the winner has none.
		let existing_account = existing.and_then(|sandbox| sandbox.account.clone());
		let existing_location = existing.and_then(|sandbox| sandbox.location.clone());
		let existing_runner = existing.and_then(|sandbox| sandbox.runner.clone());
		let (account, location, runner) = if superseded {
			(
				arg.account.clone(),
				arg.location.clone().or(existing_location),
				arg.runner.clone().or(existing_runner),
			)
		} else {
			(
				existing_account,
				existing_location.or_else(|| arg.location.clone()),
				existing_runner.or_else(|| arg.runner.clone()),
			)
		};
		if let Some(data) = &mut data {
			data.location.clone_from(&location);
		}
		let touched_at = existing.map_or(arg.touched_at, |sandbox| {
			sandbox.touched_at.max(arg.touched_at)
		});

		// Record the number of the first end of each boot, and whether this put is the first end of its boot with the number zero.
		let mut ends = existing.map_or_default(|sandbox| sandbox.ends.clone());
		let mut zero_end = None;
		if superseded
			&& let Some(data) = &arg.data
			&& data.data.status.is_stopped_or_destroyed()
			&& !ends.contains_key(&data.data.boot)
		{
			ends.insert(data.data.boot, arg.sequence);
			if arg.sequence == 0 {
				zero_end = Some(data.data.boot);
			}
		}

		// Create the record.
		let sandbox = super::Sandbox {
			account,
			created_at: existing.map_or(arg.created_at, |sandbox| sandbox.created_at),
			data,
			ends,
			location,
			reference_count: existing.map_or(0, |sandbox| sandbox.reference_count),
			runner,
			scheduler: existing
				.and_then(|sandbox| sandbox.scheduler.clone())
				.or_else(|| arg.scheduler.clone()),
			sequence,
			set: super::Set {
				processes: arg.processes.is_some()
					|| existing.is_some_and(|sandbox| sandbox.set.processes),
			},
			stopped_usages,
			touched_at,
			usages,
		};
		let merge = Merge {
			permissions,
			sandbox,
			zero_end,
		};

		Ok(Ok(merge))
	}

	/// Merge the arg's data with the existing record, keeping the data that is later in the sandbox lifecycle and combining the usage of every boot.
	#[must_use]
	fn merge_data(&self, existing: Option<&super::Sandbox>) -> DataMerge {
		// Record the usage of the arg's boot, which only grows within a boot.
		let mut usages = existing.map_or_default(|sandbox| sandbox.usages.clone());
		if let Some(data) = &self.data
			&& let Some(usage) = &data.data.usage
		{
			let usage = match usages.get(&data.data.boot) {
				Some(recorded) => max(recorded, usage),
				None => usage.clone(),
			};
			usages.insert(data.data.boot, usage);
		}

		// Keep the existing data unless the arg's data supersedes it.
		let existing_data = existing.and_then(|sandbox| sandbox.data.clone());
		let existing_sequence = existing.map_or(0, |sandbox| sandbox.sequence);
		let (mut data, sequence, superseded) = match (self.data.clone(), existing_data) {
			(Some(data), Some(existing)) => {
				if self.supersedes(&data.data, &existing.data, existing_sequence) {
					(Some(data), self.sequence, true)
				} else {
					(Some(existing), existing_sequence, false)
				}
			},
			(Some(data), None) => (Some(data), self.sequence, true),
			(None, existing) => (existing, existing_sequence, false),
		};

		// Record the storage usage of the arg's stopped boot, which only grows, unless the sandbox is destroyed, because its destruction billed its usage.
		let mut stopped_usages = existing.map_or_default(|sandbox| sandbox.stopped_usages.clone());
		let destroyed = existing
			.and_then(|sandbox| sandbox.data.as_ref())
			.is_some_and(|data| data.data.status.is_destroyed());
		if let Some(stopped) = &self.stopped_usage
			&& !destroyed
		{
			let usage = match stopped_usages.get(&stopped.boot) {
				Some(recorded) => max(recorded, &stopped.usage),
				None => stopped.usage.clone(),
			};
			stopped_usages.insert(stopped.boot, usage);
		}

		// Report the sum of the usage of every boot, while running and while stopped.
		if let Some(data) = &mut data
			&& (!usages.is_empty() || !stopped_usages.is_empty())
		{
			data.data.usage = Some(sum(usages.values().chain(stopped_usages.values())));
		}

		DataMerge {
			data,
			sequence,
			stopped_usages,
			superseded,
			usages,
		}
	}

	/// Whether the arg's data supersedes the existing data. A conditional change of a stopped sandbox, whose condition the merge checked, always applies. A destroyed sandbox is final, and a destroying sandbox only becomes destroyed. Otherwise a later boot supersedes an earlier one, the first end of a boot to arrive is final for the boot, an end supersedes the boot's unended data, and a holder's write supersedes the boot's writes with lower sequences.
	fn supersedes(
		&self,
		data: &tg::sandbox::Data,
		existing: &tg::sandbox::Data,
		existing_sequence: u64,
	) -> bool {
		if self.stopped_boot.is_some() {
			return true;
		}
		match existing.status {
			tg::sandbox::Status::Destroyed => false,
			tg::sandbox::Status::Destroying => data.status.is_destroyed(),
			tg::sandbox::Status::Started
			| tg::sandbox::Status::Starting
			| tg::sandbox::Status::Stopped
			| tg::sandbox::Status::Stopping => {
				if data.boot != existing.boot {
					data.boot > existing.boot
				} else if existing.status.is_stopped() {
					false
				} else if data.status.is_stopped_or_destroyed() {
					true
				} else {
					self.sequence > existing_sequence
				}
			},
		}
	}
}

fn max(a: &tg::sandbox::Usage, b: &tg::sandbox::Usage) -> tg::sandbox::Usage {
	let resource =
		|a: tg::sandbox::ResourceUsage, b: tg::sandbox::ResourceUsage| tg::sandbox::ResourceUsage {
			consumption: a.consumption.max(b.consumption),
			reservation: a.reservation.max(b.reservation),
		};
	tg::sandbox::Usage {
		cpu: resource(a.cpu, b.cpu),
		memory: resource(a.memory, b.memory),
		storage: resource(a.storage, b.storage),
	}
}

fn sum<'a>(usages: impl IntoIterator<Item = &'a tg::sandbox::Usage>) -> tg::sandbox::Usage {
	let resource =
		|a: tg::sandbox::ResourceUsage, b: tg::sandbox::ResourceUsage| tg::sandbox::ResourceUsage {
			consumption: a.consumption.saturating_add(b.consumption),
			reservation: a.reservation.saturating_add(b.reservation),
		};
	let mut total = tg::sandbox::Usage {
		cpu: tg::sandbox::ResourceUsage::default(),
		memory: tg::sandbox::ResourceUsage::default(),
		storage: tg::sandbox::ResourceUsage::default(),
	};
	for usage in usages {
		total = tg::sandbox::Usage {
			cpu: resource(total.cpu, usage.cpu),
			memory: resource(total.memory, usage.memory),
			storage: resource(total.storage, usage.storage),
		};
	}
	total
}
