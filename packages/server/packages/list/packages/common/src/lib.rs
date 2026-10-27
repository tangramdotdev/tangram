use tangram_client::prelude::*;

pub struct Kinds {
	pub groups: bool,
	pub organizations: bool,
	pub tags: bool,
	pub users: bool,
}

#[must_use]
pub fn entry_kind_enabled(entry: &tg::list::Entry, kinds: &Kinds) -> bool {
	match entry.kind() {
		tg::id::Kind::Group => kinds.groups,
		tg::id::Kind::Organization => kinds.organizations,
		tg::id::Kind::Tag => kinds.tags,
		tg::id::Kind::User => kinds.users,
		_ => false,
	}
}

#[must_use]
pub fn sort_entries(mut data: Vec<tg::list::Entry>, reverse: bool) -> Vec<tg::list::Entry> {
	data.sort_by(|a, b| compare_entries(a, b, reverse));
	data
}

#[must_use]
pub fn compare_entries(
	a: &tg::list::Entry,
	b: &tg::list::Entry,
	reverse: bool,
) -> std::cmp::Ordering {
	let order = tg::list::compare(&a.specifier().to_string(), &b.specifier().to_string());
	let order = if reverse { order.reverse() } else { order };
	order
		.then_with(|| entry_kind(a).cmp(&entry_kind(b)))
		.then_with(|| {
			let order = a.specifier().to_string().cmp(&b.specifier().to_string());
			if reverse { order.reverse() } else { order }
		})
}

#[must_use]
pub fn entry_kind(entry: &tg::list::Entry) -> tg::id::Kind {
	entry.kind()
}
