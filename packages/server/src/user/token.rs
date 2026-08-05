use tangram_client::prelude::*;

pub mod create;
pub mod delete;
pub mod list;

impl crate::Session {
	pub(super) fn authenticated_user(&self) -> tg::Result<&tg::user::Id> {
		let tg::Principal::User(user) = &self.context.principal else {
			return Err(tg::error!("unauthorized"));
		};

		Ok(user)
	}
}
