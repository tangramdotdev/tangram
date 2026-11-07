use tangram_client::prelude::*;

pub mod token;

#[must_use]
pub fn deleted_users(batch: &tangram_index::write::Arg) -> Vec<tg::user::Id> {
	batch
		.items
		.iter()
		.filter_map(|item| match item {
			tangram_index::Item::DeleteUser(arg) => Some(arg.id.clone()),
			_ => None,
		})
		.collect()
}
