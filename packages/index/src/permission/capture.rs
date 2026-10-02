use tangram_client::prelude::*;

pub mod enqueue;

#[derive(Clone, Debug)]
pub struct Entry {
	pub arg: enqueue::Arg,
	pub partition: u64,
}

pub fn validate_permissions(
	resource: &tg::Id,
	permissions: tg::authorization::permission::Set,
) -> tg::Result<()> {
	let allowed = if tg::object::Id::try_from(resource.clone()).is_ok() {
		{
			let mut allowed = tg::authorization::permission::object::Set::NODE;
			allowed.insert(tg::authorization::permission::object::Set::SUBTREE);
			tg::authorization::permission::Set::Object(allowed)
		}
	} else if tg::process::Id::try_from(resource.clone()).is_ok() {
		{
			let mut allowed = tg::authorization::permission::process::Set::all();
			allowed.remove(tg::authorization::permission::process::Set::PARENT);
			tg::authorization::permission::Set::Process(allowed)
		}
	} else {
		return Err(tg::error!(
			"a permission capture resource must be an object or process"
		));
	};
	if !allowed.contains(permissions) {
		return Err(tg::error!(
			"a permission capture permission must be a read permission"
		));
	}
	Ok(())
}
