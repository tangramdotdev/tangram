use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub permissions: tg::grant::permission::Set,
	pub resource: tg::grant::Resource,
	pub token: Option<tg::grant::Body>,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub permissions: tg::grant::permission::Set,
}

#[derive(Clone, Copy, Debug)]
pub struct ObjectSubtreeConfig {
	pub max_depth: usize,
	pub max_objects: usize,
}

impl Default for ObjectSubtreeConfig {
	fn default() -> Self {
		Self {
			max_depth: 16,
			max_objects: 1024,
		}
	}
}

/// Validate that the permission is coherent with the resource kind.
pub fn validate_permission(resource: &tg::Id, permission: tg::grant::Permission) -> tg::Result<()> {
	let valid = match permission {
		tg::grant::Permission::Group(_) => resource.kind() == tg::id::Kind::Group,
		tg::grant::Permission::Object(_) => tg::object::Id::try_from(resource.clone()).is_ok(),
		tg::grant::Permission::Organization(_) => resource.kind() == tg::id::Kind::Organization,
		tg::grant::Permission::Process(_) => resource.kind() == tg::id::Kind::Process,
		tg::grant::Permission::Sandbox(_) => resource.kind() == tg::id::Kind::Sandbox,
		tg::grant::Permission::Tag(_) => resource.kind() == tg::id::Kind::Tag,
		tg::grant::Permission::User(_) => resource.kind() == tg::id::Kind::User,
	};
	if !valid {
		return Err(tg::error!(%resource, %permission, "invalid permission for the resource"));
	}
	Ok(())
}

pub fn validate(resource: &tg::Id, permissions: tg::grant::permission::Set) -> tg::Result<()> {
	for permission in permissions.iter() {
		validate_permission(resource, permission)?;
	}
	Ok(())
}
