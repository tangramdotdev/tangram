use {crate::Component, tangram_client::prelude::*, tangram_server_core::Session};

pub mod create;
pub mod delete;
pub mod list;

impl Component {
	pub(super) fn authenticated_user(session: &Session) -> tg::Result<&tg::user::Id> {
		let tg::Principal::User(user) = &session.context.principal else {
			return Err(tg::error!("unauthorized"));
		};

		Ok(user)
	}
}
