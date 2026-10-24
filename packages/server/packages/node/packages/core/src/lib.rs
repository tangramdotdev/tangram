use tangram_client::prelude::*;

pub fn delete_permission_for_named_node(id: &tg::Id) -> tg::Result<tg::authorization::Permission> {
	match id.kind() {
		tg::id::Kind::Group => Ok(tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Admin,
		)),
		tg::id::Kind::Organization => Ok(tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Admin,
		)),
		tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Write,
		)),
		tg::id::Kind::User => Ok(tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Admin,
		)),
		_ => Err(tg::error!("invalid named node kind")),
	}
}
