use {crate::Session, tangram_client::prelude::*};

mod get;
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
			.get_user(token)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the user"))?
		else {
			return Ok(None);
		};
		Ok(Some(Some(user)))
	}
}
