use {
	crate::usage::Account,
	std::collections::{HashMap, HashSet},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct ObjectArg {
	#[tangram_serialize(id = 1)]
	pub account: Account,

	#[tangram_serialize(id = 0)]
	pub object: tg::object::Id,

	#[tangram_serialize(id = 2)]
	pub touched_at: i64,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct ProcessArg {
	#[tangram_serialize(id = 0)]
	pub account: Account,

	#[tangram_serialize(id = 1)]
	pub process: tg::process::Id,

	#[tangram_serialize(id = 2)]
	pub touched_at: i64,
}

/// Give the account to every object put that is reachable from the roots through the puts, and put the account's storage entry for every reachable object that is not put, so that the index does not propagate the account's storage through the puts.
pub fn put_account_objects(
	items: &mut Vec<crate::Item>,
	account: &Account,
	roots: &[tg::object::Id],
	touched_at: i64,
) {
	// Index the object puts.
	let mut puts = items
		.iter_mut()
		.filter_map(|item| match item {
			crate::Item::PutObject(arg) => Some((arg.id.clone(), arg)),
			_ => None,
		})
		.collect::<HashMap<_, _>>();

	// Visit the objects that are reachable from the roots through the puts.
	let mut entries = Vec::new();
	let mut visited = HashSet::new();
	let mut stack = roots.to_vec();
	while let Some(id) = stack.pop() {
		if !visited.insert(id.clone()) {
			continue;
		}
		if let Some(arg) = puts.get_mut(&id) {
			arg.account = Some(account.clone());
			stack.extend(arg.children.iter().cloned());
		} else {
			let entry = ObjectArg {
				account: account.clone(),
				object: id,
				touched_at,
			};
			entries.push(crate::Item::PutAccountObject(Box::new(entry)));
		}
	}

	items.extend(entries);
}
