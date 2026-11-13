use {
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
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

	/// Permissions granted after validation if the sandbox is new or both `data` and `processes` are `Some`.
	#[tangram_serialize(id = 8)]
	pub permissions: Vec<crate::permission::put::Arg>,

	#[tangram_serialize(id = 9)]
	pub principal: tg::Principal,

	/// The process that the sandbox is created with, which is put after the sandbox, in the same transaction.
	#[tangram_serialize(default, id = 13, skip_serializing_if = "Option::is_none")]
	pub process: Option<Box<crate::process::put::Arg>>,

	#[tangram_serialize(id = 7)]
	pub processes: Option<Vec<tg::process::Id>>,

	/// The registration that must still own the runner when this mutation is applied.
	#[tangram_serialize(default, id = 11, skip_serializing_if = "Option::is_none")]
	pub registration: Option<crate::runner::Registration>,

	#[tangram_serialize(id = 4)]
	pub runner: Option<tg::runner::Id>,

	/// The boot that the sandbox must be stopped at when this put is applied, which makes the put a conditional change of a stopped sandbox, such as a start that reserves the next boot or a destroy, so that of several such changes only the first is applied.
	#[tangram_serialize(default, id = 14, skip_serializing_if = "Option::is_none")]
	pub stopped_boot: Option<u64>,

	/// The storage usage of a boot's stopped directory so far, which only grows.
	#[tangram_serialize(default, id = 10, skip_serializing_if = "Option::is_none")]
	pub stopped_usage: Option<StoppedUsage>,

	#[tangram_serialize(id = 5)]
	pub touched_at: i64,
}

/// The merged sandbox data and per-boot usage that a put records.
pub struct Merge {
	pub data: Option<tg::sandbox::get::Output>,
	pub stopped_usages: BTreeMap<u64, tg::sandbox::Usage>,
	pub usages: BTreeMap<u64, tg::sandbox::Usage>,
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

	/// Merge the arg's data with the existing record, keeping the data that is later in the sandbox lifecycle and combining the usage of every boot.
	#[must_use]
	pub fn merge(&self, existing: Option<&super::Sandbox>) -> Merge {
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

		// Keep the existing data when the arg's data is earlier in the lifecycle.
		let existing_data = existing.and_then(|sandbox| sandbox.data.clone());
		let mut data = match (self.data.clone(), existing_data) {
			(Some(data), Some(existing)) => {
				if position(&data.data) < position(&existing.data) {
					Some(existing)
				} else {
					Some(data)
				}
			},
			(Some(data), None) => Some(data),
			(None, existing) => existing,
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

		Merge {
			data,
			stopped_usages,
			usages,
		}
	}
}

/// The position of the sandbox data in its lifecycle, which orders writes that may be applied out of order. A destroying or destroyed sandbox is after every boot.
#[must_use]
fn position(data: &tg::sandbox::Data) -> (u64, u8) {
	match data.status {
		tg::sandbox::Status::Starting => (data.boot, 0),
		tg::sandbox::Status::Started => (data.boot, 1),
		tg::sandbox::Status::Stopping => (data.boot, 2),
		tg::sandbox::Status::Stopped => (data.boot, 3),
		tg::sandbox::Status::Destroying => (u64::MAX, 4),
		tg::sandbox::Status::Destroyed => (u64::MAX, 5),
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
