use tangram_client::prelude::*;

pub struct Kinds {
	pub groups: bool,
	pub organizations: bool,
	pub tags: bool,
	pub users: bool,
}

#[must_use]
pub fn item_kind_enabled(item: &tg::list::Item, kinds: &Kinds) -> bool {
	match item.kind() {
		tg::id::Kind::Group => kinds.groups,
		tg::id::Kind::Organization => kinds.organizations,
		tg::id::Kind::Tag => kinds.tags,
		tg::id::Kind::User => kinds.users,
		_ => false,
	}
}
