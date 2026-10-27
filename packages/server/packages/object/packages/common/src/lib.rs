use tangram_client::prelude::*;

#[must_use]
pub fn mask_object_metadata_with_permissions(
	metadata: tg::object::Metadata,
	permissions: tg::authorization::permission::Set,
) -> Option<tg::object::Metadata> {
	let subtree = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	);
	if permissions.contains(subtree) {
		return Some(metadata);
	}
	let node = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Node,
	);
	permissions
		.contains(node)
		.then(tg::object::Metadata::default)
}
