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

	#[tangram_serialize(id = 7)]
	pub processes: Option<Vec<tg::process::Id>>,
	#[tangram_serialize(id = 4)]
	pub runner: Option<tg::runner::Id>,
	#[tangram_serialize(id = 5)]
	pub touched_at: i64,
}

/// The merged sandbox data and per-boot usage that a put records.
pub struct Merge {
	pub data: Option<tg::sandbox::get::Output>,
	pub usages: BTreeMap<u64, tg::sandbox::Usage>,
}

/// The position of the sandbox data in its lifecycle, which orders writes that may be applied out of order. A destroyed sandbox is after every boot.
#[must_use]
pub fn position(data: &tg::sandbox::Data) -> (bool, u64, u8) {
	let rank = match data.status {
		tg::sandbox::Status::Starting => 0,
		tg::sandbox::Status::Started => 1,
		tg::sandbox::Status::Stopping => 2,
		tg::sandbox::Status::Stopped => 3,
		tg::sandbox::Status::Destroyed => 4,
	};
	(data.status.is_destroyed(), data.boot, rank)
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
		let mut usages = existing
			.map(|sandbox| sandbox.usages.clone())
			.unwrap_or_default();
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

		// Report the sum of the usage of every boot.
		if let Some(data) = &mut data
			&& !usages.is_empty()
		{
			data.data.usage = Some(sum(usages.values()));
		}

		Merge { data, usages }
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
