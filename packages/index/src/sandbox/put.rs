use {std::collections::BTreeSet, tangram_client::prelude::*};

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
}
