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

#[must_use]
pub fn sort_items(mut data: Vec<tg::list::Item>, reverse: bool) -> Vec<tg::list::Item> {
	data.sort_by(|a, b| compare_items(a, b, reverse));
	data
}

#[must_use]
pub fn compare_items(a: &tg::list::Item, b: &tg::list::Item, reverse: bool) -> std::cmp::Ordering {
	let order = tg::list::compare(&a.specifier().to_string(), &b.specifier().to_string());
	let order = if reverse { order.reverse() } else { order };
	order
		.then_with(|| item_kind(a).cmp(&item_kind(b)))
		.then_with(|| {
			let order = a.specifier().to_string().cmp(&b.specifier().to_string());
			if reverse { order.reverse() } else { order }
		})
}

#[must_use]
pub fn item_kind(item: &tg::list::Item) -> tg::id::Kind {
	item.kind()
}
