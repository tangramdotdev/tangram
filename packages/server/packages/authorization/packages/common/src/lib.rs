use tangram_client::prelude::*;

pub fn permission_capture_permissions(
	resource: &tg::Id,
) -> tg::Result<tg::authorization::permission::Set> {
	if resource.kind().is_object() {
		let mut permissions = tg::authorization::permission::object::Set::NODE;
		permissions.insert(tg::authorization::permission::object::Set::SUBTREE);
		return Ok(tg::authorization::permission::Set::Object(permissions));
	}
	if resource.kind() == tg::id::Kind::Process {
		let mut permissions = tg::authorization::permission::process::Set::all();
		permissions.remove(tg::authorization::permission::process::Set::PARENT);
		return Ok(tg::authorization::permission::Set::Process(permissions));
	}
	Err(tg::error!("invalid permission capture resource"))
}
