use {
	crate::Session, tangram_client::prelude::*, tangram_futures::stream::TryExt,
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) fn create_token(
		&self,
		id: tg::Id,
		permissions: Vec<tg::grant::Permission>,
		expires_at: i64,
	) -> tg::Result<Option<tg::Token>> {
		let Some(private_key) = self.server.tokens.private_key.as_ref() else {
			return Ok(None);
		};
		let body = tg::token::Body {
			expires_at,
			id,
			permissions,
		};
		let token = tg::Token::sign(body, private_key)?;
		Ok(Some(token))
	}

	pub(crate) async fn authorize(
		&self,
		resource: tg::grant::Resource,
		permissions: impl Into<tg::grant::permission::Set>,
	) -> tg::Result<Option<tg::grant::permission::Set>> {
		let permissions = permissions.into();

		// Authorize the root principal for all resources.
		if matches!(self.context.principal, Some(tg::Principal::Root)) {
			return Ok(Some(permissions));
		}

		// Authorize a sandbox for its own processes.
		if let (
			tg::grant::Resource::Id(id),
			tg::grant::permission::Set::Process(_),
			Some(tg::Principal::Sandbox(sandbox)),
		) = (&resource, permissions, self.context.principal.as_ref())
			&& let Ok(process) = tg::process::Id::try_from(id.clone())
			&& let Some(output) = self.server.try_get_process_local(&process, false).await?
			&& output.data.sandbox == *sandbox
		{
			return Ok(Some(permissions));
		}

		// Attempt to authorize.
		if let Some(output) = self
			.server
			.index
			.authorize(
				resource.clone(),
				permissions,
				self.context.principal.as_ref(),
			)
			.await? && !output.permissions.is_empty()
		{
			return Ok(Some(output.permissions));
		}

		// Index.
		self.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;

		// Attempt to authorize again.
		let output = self
			.server
			.index
			.authorize(resource, permissions, self.context.principal.as_ref())
			.await?;

		Ok(output.map(|output| output.permissions))
	}
}
