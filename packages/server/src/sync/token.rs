use {crate::Session, tangram_client::prelude::*};

impl Session {
	pub(crate) fn create_sync_token(&self) -> tg::Result<Option<tg::sync::Token>> {
		let Some(private_key) = self.server.authorization_tokens.private_key.as_ref() else {
			return Ok(None);
		};
		let expires_at = self.server.clock.unix_timestamp()?
			+ i64::try_from(self.server.config.sync.grant_time_to_live.as_secs())
				.map_err(|error| tg::error!(!error, "failed to convert the time to live"))?;
		let body = tg::sync::token::Body::new(expires_at);
		let token = tg::sync::Token::sign(body, private_key)?;
		Ok(Some(token))
	}

	pub(crate) fn verify_sync_token(&self, token: &tg::sync::Token) -> bool {
		let Ok(now) = self.server.clock.unix_timestamp() else {
			return false;
		};
		let Some(public_key) = self
			.server
			.authorization_tokens
			.public_keys
			.get(&token.metadata.key)
		else {
			return false;
		};
		token.verify_at(public_key, now).is_ok()
	}

	pub(crate) fn has_verified_sync_token(&self, tokens: &tg::Tokens) -> bool {
		tokens
			.local_sync()
			.is_some_and(|token| self.verify_sync_token(token))
	}
}
