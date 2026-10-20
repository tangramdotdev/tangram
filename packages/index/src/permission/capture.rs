use tangram_client::prelude::*;

pub mod enqueue;
pub mod put;

#[derive(Clone, Debug)]
pub struct Entry {
	/// Identifies an enqueue so completion cannot remove a newer notification.
	pub attempt: u128,
	pub partition: u64,
	pub resource: tg::Id,
}

pub fn requested_permissions(resource: &tg::Id) -> tg::Result<tg::authorization::permission::Set> {
	let allowed = if resource.kind().is_object() {
		let mut allowed = tg::authorization::permission::object::Set::NODE;
		allowed.insert(tg::authorization::permission::object::Set::SUBTREE);
		tg::authorization::permission::Set::Object(allowed)
	} else if resource.kind() == tg::id::Kind::Process {
		let mut allowed = tg::authorization::permission::process::Set::all();
		allowed.remove(tg::authorization::permission::process::Set::PARENT);
		tg::authorization::permission::Set::Process(allowed)
	} else {
		return Err(tg::error!(
			"a permission capture resource must be an object or process"
		));
	};

	Ok(allowed)
}

pub fn validate_permissions(
	resource: &tg::Id,
	permissions: tg::authorization::permission::Set,
) -> tg::Result<()> {
	if !requested_permissions(resource)?.contains(permissions) {
		return Err(tg::error!(
			"a permission capture permission must be a read permission"
		));
	}
	Ok(())
}
