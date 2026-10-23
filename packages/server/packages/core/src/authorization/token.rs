use {crate::Session, num::ToPrimitive as _, tangram_client::prelude::*};

impl Session {
	pub fn add_token_to_object_referent<T>(&self, referent: &mut tg::Referent<T>) -> tg::Result<()>
	where
		T: Clone + Into<tg::Id>,
	{
		let expires_at = self.server.clock.unix_timestamp()
			+ self
				.server
				.config
				.object
				.permission_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		self.add_token_to_object_referent_with_expires_at(referent, expires_at)?;
		Ok(())
	}

	pub fn add_token_to_object_referent_with_expires_at<T>(
		&self,
		referent: &mut tg::Referent<T>,
		expires_at: i64,
	) -> tg::Result<()>
	where
		T: Clone + Into<tg::Id>,
	{
		let token = self.create_token(
			referent.node.clone().into(),
			vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			)],
			expires_at,
		)?;
		if let Some(token) = token {
			referent.options.tokens.insert_local_authorization(token);
		}
		Ok(())
	}
}
