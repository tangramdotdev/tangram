use {crate::Session, tangram_client::prelude::*};

mod current;
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
		let Some(token) = self.context.token.as_ref() else {
			return Ok(None);
		};
		let Some(user) = self
			.get_current_user_local(token)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the current user"))?
		else {
			return Ok(None);
		};
		Ok(Some(Some(user)))
	}
}
