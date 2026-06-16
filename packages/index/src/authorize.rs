use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub permissions: tg::grant::Set,
	pub resource: tg::grant::Resource,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub permissions: tg::grant::Set,
}

/// Validate that the permission is coherent with the resource kind.
pub fn validate_permission(resource: &tg::Id, permission: tg::grant::Permission) -> tg::Result<()> {
	let valid = match permission {
		tg::grant::Permission::Admin
		| tg::grant::Permission::Read
		| tg::grant::Permission::Write => {
			matches!(
				resource.kind(),
				tg::id::Kind::User
					| tg::id::Kind::Group
					| tg::id::Kind::Organization
					| tg::id::Kind::Tag
			)
		},
		tg::grant::Permission::Object(_) => tg::object::Id::try_from(resource.clone()).is_ok(),
		tg::grant::Permission::Process(_) => resource.kind() == tg::id::Kind::Process,
	};
	if !valid {
		return Err(tg::error!(%resource, %permission, "invalid permission for the resource"));
	}
	Ok(())
}

pub fn validate(resource: &tg::Id, permissions: tg::grant::Set) -> tg::Result<()> {
	for permission in permissions.iter() {
		validate_permission(resource, permission)?;
	}
	Ok(())
}
