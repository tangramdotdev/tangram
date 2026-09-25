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
	#[tangram_serialize(id = 7)]
	pub processes: Option<Vec<tg::process::Id>>,
	#[tangram_serialize(id = 4)]
	pub runner: Option<tg::runner::Id>,
	#[tangram_serialize(id = 5)]
	pub touched_at: i64,
}

impl Arg {
	pub fn validate(&self) -> tg::Result<()> {
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
}
