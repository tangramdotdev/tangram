use {crate::Session, tangram_client::prelude::*};

mod current;
mod grant;
mod login;

impl Session {
	pub(crate) async fn authorize(&self) -> tg::Result<Option<tg::User>> {
		let Some(user) = self
			.try_authorize()
			.await?
			.ok_or_else(|| tg::error!("failed to authorize"))?
		else {
			return Ok(None);
		};
		Ok(Some(user))
	}

	pub(crate) async fn try_authorize(&self) -> tg::Result<Option<Option<tg::User>>> {
		if !self.server.config().authorization {
			return Ok(Some(None));
		}
		Ok(self.context.user.clone().map(Some))
	}
}
