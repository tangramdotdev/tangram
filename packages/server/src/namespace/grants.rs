use {crate::Session, tangram_client::prelude::*};

pub mod create;
pub mod delete;
pub mod list;

pub(super) enum Grantee {
	User(String),
	Group(String),
	Public,
}

impl Session {
	pub(super) fn grantee(
		user: Option<String>,
		group: Option<String>,
		public: bool,
	) -> tg::Result<Grantee> {
		match (user, group, public) {
			(Some(user), None, false) => Ok(Grantee::User(user)),
			(None, Some(group), false) => Ok(Grantee::Group(group)),
			(None, None, true) => Ok(Grantee::Public),
			_ => Err(tg::error!("expected exactly one grantee")),
		}
	}
}
