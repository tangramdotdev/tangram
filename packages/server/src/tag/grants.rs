use {crate::Session, tangram_client::prelude::*};

pub mod create;
pub mod delete;
pub mod list;

pub(super) enum Grantee {
	User(String),
	Group(String),
	All,
}

impl Session {
	pub(super) fn tag_grantee(
		user: Option<String>,
		group: Option<String>,
		all: bool,
	) -> tg::Result<Grantee> {
		match (user, group, all) {
			(Some(user), None, false) => Ok(Grantee::User(user)),
			(None, Some(group), false) => Ok(Grantee::Group(group)),
			(None, None, true) => Ok(Grantee::All),
			_ => Err(tg::error!("expected exactly one grantee")),
		}
	}
}
