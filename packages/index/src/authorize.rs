use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub permissions: tg::authorization::permission::Set,
	pub resource: tg::Selector<tg::Id>,
	pub token: Option<tg::authorization::Body>,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub permissions: tg::authorization::permission::Set,
}

#[derive(Clone, Copy, Debug)]
pub struct ObjectSubtreeConfig {
	pub max_depth: usize,
	pub max_objects: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct ProcessSubtreeConfig {
	pub max_depth: usize,
	pub max_processes: usize,
}

enum NamedPermission {
	Admin,
	Read,
	Write,
}

impl Default for ObjectSubtreeConfig {
	fn default() -> Self {
		Self {
			max_depth: 16,
			max_objects: 1024,
		}
	}
}

impl Default for ProcessSubtreeConfig {
	fn default() -> Self {
		Self {
			max_depth: 16,
			max_processes: 1024,
		}
	}
}

/// Validate that the permission is coherent with the resource kind.
pub fn validate_permission(
	resource: &tg::Id,
	permission: tg::authorization::Permission,
) -> tg::Result<()> {
	let valid = match permission {
		tg::authorization::Permission::Group(_) => resource.kind() == tg::id::Kind::Group,
		tg::authorization::Permission::Object(_) => {
			tg::object::Id::try_from(resource.clone()).is_ok()
		},
		tg::authorization::Permission::Organization(_) => {
			resource.kind() == tg::id::Kind::Organization
		},
		tg::authorization::Permission::Process(_) => resource.kind() == tg::id::Kind::Process,
		tg::authorization::Permission::Sandbox(_) => resource.kind() == tg::id::Kind::Sandbox,
		tg::authorization::Permission::Tag(_) => resource.kind() == tg::id::Kind::Tag,
		tg::authorization::Permission::User(_) => resource.kind() == tg::id::Kind::User,
	};
	if !valid {
		return Err(tg::error!(%resource, %permission, "invalid permission for the resource"));
	}
	Ok(())
}

pub fn validate(
	resource: &tg::Id,
	permissions: tg::authorization::permission::Set,
) -> tg::Result<()> {
	for permission in permissions.iter() {
		validate_permission(resource, permission)?;
	}
	Ok(())
}

pub(crate) fn permissions_for_specifier_prefix(
	resource: &tg::Id,
	permissions: tg::authorization::permission::Set,
) -> tg::Result<Option<tg::authorization::permission::Set>> {
	let mut permissions_ = permissions.iter();
	let Some(permission) = permissions_.next() else {
		return Ok(None);
	};
	if permissions_.next().is_some()
		|| !matches!(
			permission,
			tg::authorization::Permission::Group(
				tg::authorization::permission::group::Permission::Write
			) | tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Write
			)
		) {
		return Ok(None);
	}
	let permission = write_permission_for_resource(resource)?;
	let permissions = tg::authorization::permission::Set::from_permission(permission);

	Ok(Some(permissions))
}

pub(crate) fn write_permission_for_resource(
	resource: &tg::Id,
) -> tg::Result<tg::authorization::Permission> {
	match resource.kind() {
		tg::id::Kind::Group => Ok(tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Write,
		)),
		tg::id::Kind::Organization => Ok(tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Write,
		)),
		tg::id::Kind::Process => Ok(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Write,
		)),
		tg::id::Kind::Sandbox => Ok(tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Write,
		)),
		tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Write,
		)),
		tg::id::Kind::User => Ok(tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Write,
		)),
		_ => Err(tg::error!(%resource, "invalid resource")),
	}
}

pub(crate) fn permission_for_named_parent(
	parent: &tg::Id,
	permission: tg::authorization::Permission,
) -> tg::Result<tg::authorization::Permission> {
	let permission = match permission {
		tg::authorization::Permission::Group(permission) => match permission {
			tg::authorization::permission::group::Permission::Admin => NamedPermission::Admin,
			tg::authorization::permission::group::Permission::Read => NamedPermission::Read,
			tg::authorization::permission::group::Permission::Write => NamedPermission::Write,
		},
		tg::authorization::Permission::Organization(permission) => match permission {
			tg::authorization::permission::organization::Permission::Admin => {
				NamedPermission::Admin
			},
			tg::authorization::permission::organization::Permission::Read => NamedPermission::Read,
			tg::authorization::permission::organization::Permission::Write => {
				NamedPermission::Write
			},
		},
		tg::authorization::Permission::Tag(permission) => match permission {
			tg::authorization::permission::tag::Permission::Admin => NamedPermission::Admin,
			tg::authorization::permission::tag::Permission::Read => NamedPermission::Read,
			tg::authorization::permission::tag::Permission::Write => NamedPermission::Write,
		},
		tg::authorization::Permission::User(permission) => match permission {
			tg::authorization::permission::user::Permission::Admin => NamedPermission::Admin,
			tg::authorization::permission::user::Permission::Read => NamedPermission::Read,
			tg::authorization::permission::user::Permission::Write => NamedPermission::Write,
		},
		_ => return Err(tg::error!(%parent, %permission, "invalid named node permission")),
	};
	let permission = match (parent.kind(), permission) {
		(tg::id::Kind::Group, NamedPermission::Admin) => tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Admin,
		),
		(tg::id::Kind::Group, NamedPermission::Read) => tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Read,
		),
		(tg::id::Kind::Group, NamedPermission::Write) => tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Write,
		),
		(tg::id::Kind::Organization, NamedPermission::Admin) => {
			tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Admin,
			)
		},
		(tg::id::Kind::Organization, NamedPermission::Read) => {
			tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Read,
			)
		},
		(tg::id::Kind::Organization, NamedPermission::Write) => {
			tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Write,
			)
		},
		(tg::id::Kind::Tag, NamedPermission::Admin) => tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Admin,
		),
		(tg::id::Kind::Tag, NamedPermission::Read) => {
			tg::authorization::Permission::Tag(tg::authorization::permission::tag::Permission::Read)
		},
		(tg::id::Kind::Tag, NamedPermission::Write) => tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Write,
		),
		(tg::id::Kind::User, NamedPermission::Admin) => tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Admin,
		),
		(tg::id::Kind::User, NamedPermission::Read) => tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Read,
		),
		(tg::id::Kind::User, NamedPermission::Write) => tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Write,
		),
		_ => return Err(tg::error!(%parent, "invalid named node parent")),
	};

	Ok(permission)
}
