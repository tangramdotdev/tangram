use {std::collections::BTreeSet, tangram_client::prelude::*};

pub struct Options {
	pub command_sync: Option<tg::Referent<tg::sync::Id>>,
	pub defer_index: bool,
	pub location: Option<tg::Location>,
	pub outcome_sync: Option<tg::Referent<tg::sync::Id>>,
	pub store_data: bool,
}

pub fn validate_process_data(data: &tg::process::Data) -> tg::Result<()> {
	if data.status != tg::process::Status::Finished {
		return Err(tg::error!("expected a finished process"));
	}
	if let Some(children) = &data.children {
		let mut ids = BTreeSet::new();
		for child in children {
			if !ids.insert(&child.process.node) {
				return Err(tg::error!("the process children must be unique"));
			}
		}
	}

	Ok(())
}
