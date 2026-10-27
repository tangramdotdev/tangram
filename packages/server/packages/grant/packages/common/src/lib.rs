use tangram_client::prelude::*;

pub fn admin_permission_for_resource(
	resource: &tg::Id,
) -> tg::Result<tg::authorization::Permission> {
	match resource.kind() {
		tg::id::Kind::Group => Ok(tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Admin,
		)),
		tg::id::Kind::Organization => Ok(tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Admin,
		)),
		tg::id::Kind::Sandbox => Ok(tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Parent,
		)),
		tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Admin,
		)),
		tg::id::Kind::User => Ok(tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Admin,
		)),
		_ => Err(tg::error!("invalid resource")),
	}
}

pub fn read_permission_for_resource(
	resource: &tg::Id,
) -> tg::Result<tg::authorization::Permission> {
	match resource.kind() {
		tg::id::Kind::Group => Ok(tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Read,
		)),
		tg::id::Kind::Organization => Ok(tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Read,
		)),
		tg::id::Kind::Process => Ok(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		)),
		tg::id::Kind::Sandbox => Ok(tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Node,
		)),
		tg::id::Kind::Sync => Ok(tg::authorization::Permission::Sync(
			tg::authorization::permission::sync::Permission::Read,
		)),
		tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Read,
		)),
		tg::id::Kind::User => Ok(tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Read,
		)),
		_ => Err(tg::error!("invalid resource")),
	}
}

pub fn write_permission_for_resource(
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
			tg::authorization::permission::process::Permission::Parent,
		)),
		tg::id::Kind::Sandbox => Ok(tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Parent,
		)),
		tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Write,
		)),
		tg::id::Kind::User => Ok(tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Write,
		)),
		_ => Err(tg::error!("invalid resource")),
	}
}
