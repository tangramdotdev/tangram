use {std::collections::BTreeMap, tangram_client::prelude::*};

#[must_use]
pub fn children<'a>(
	error: &tg::Error,
	modules: impl IntoIterator<Item = &'a tg::module::Data>,
) -> Vec<tg::Referent<tg::object::Id>> {
	// Collect each child once, including children of inline error sources.
	let Some(object) = error.state().object() else {
		return Vec::new();
	};
	let mut children = BTreeMap::<tg::object::Id, tg::Referent<tg::object::Id>>::new();
	for child in object.children() {
		let child = child.to_referent();
		children
			.entry(child.node.clone())
			.and_modify(|entry| entry.inherit(&child))
			.or_insert(child);
	}

	// Recover module proofs at the output boundary without adding them to stack frames.
	for module in modules {
		let mut referents = Vec::new();
		module.children_with_tokens(&mut referents);
		for referent in referents {
			if let Some(child) = children.get_mut(&referent.node) {
				child.inherit(&referent);
			}
		}
	}

	children.into_values().collect()
}
