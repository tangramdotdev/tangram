use {
	crate::Session, tangram_client::prelude::*, tangram_futures::stream::TryExt,
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) fn create_token(
		&self,
		resource: tg::grant::Resource,
		permissions: Vec<tg::grant::Permission>,
		expires_at: i64,
	) -> tg::Result<Option<tg::Token>> {
		let Some(private_key) = self.server.tokens.private_key.as_ref() else {
			return Ok(None);
		};
		let body = tg::token::Body {
			expires_at,
			permissions,
			resource,
		};
		let token = tg::Token::sign(body, private_key)?;
		Ok(Some(token))
	}

	pub(crate) async fn authorize(
		&self,
		resource: impl IntoAuthorizationResource,
		permissions: impl Into<tg::grant::permission::Set>,
	) -> tg::Result<Option<tg::grant::permission::Set>> {
		let (resource, token) = resource.into_authorization_resource();
		let permissions = permissions.into();

		if let Some(token) = token
			&& self.authorize_token(&resource, permissions, &token)
		{
			return Ok(Some(permissions));
		}

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

	fn authorize_token(
		&self,
		resource: &tg::grant::Resource,
		permissions: tg::grant::permission::Set,
		token: &tg::Token,
	) -> bool {
		if token.body.resource != *resource {
			return false;
		}
		let Some(public_key) = self.server.tokens.public_keys.get(&token.metadata.key) else {
			return false;
		};
		if token.verify(public_key).is_err() {
			return false;
		}
		permissions
			.iter()
			.all(|permission| token.body.grants(permission))
	}
}

pub(crate) trait IntoResource {
	fn into_resource(self) -> tg::grant::Resource;
}

pub(crate) trait IntoAuthorizationResource {
	fn into_authorization_resource(self) -> (tg::grant::Resource, Option<tg::Token>);
}

impl IntoResource for tg::grant::Resource {
	fn into_resource(self) -> tg::grant::Resource {
		self
	}
}

impl IntoResource for tg::Id {
	fn into_resource(self) -> tg::grant::Resource {
		tg::grant::Resource::Id(self)
	}
}

impl IntoResource for tg::object::Id {
	fn into_resource(self) -> tg::grant::Resource {
		tg::grant::Resource::Id(self.into())
	}
}

impl IntoResource for tg::process::Id {
	fn into_resource(self) -> tg::grant::Resource {
		tg::grant::Resource::Id(self.into())
	}
}

impl IntoResource for tg::artifact::Id {
	fn into_resource(self) -> tg::grant::Resource {
		tg::grant::Resource::Id(tg::object::Id::from(self).into())
	}
}

impl<I> IntoResource for tg::Selector<I>
where
	I: Into<tg::Id>,
{
	fn into_resource(self) -> tg::grant::Resource {
		self.into()
	}
}

impl<T> IntoAuthorizationResource for T
where
	T: IntoResource,
{
	fn into_authorization_resource(self) -> (tg::grant::Resource, Option<tg::Token>) {
		(self.into_resource(), None)
	}
}

impl<T> IntoAuthorizationResource for tg::WithToken<T>
where
	T: IntoResource,
{
	fn into_authorization_resource(self) -> (tg::grant::Resource, Option<tg::Token>) {
		(self.id.into_resource(), Some(self.token))
	}
}

impl<T> IntoAuthorizationResource for tg::MaybeWithToken<T>
where
	T: IntoResource,
{
	fn into_authorization_resource(self) -> (tg::grant::Resource, Option<tg::Token>) {
		match self {
			tg::Either::Left(resource) => (resource.into_resource(), None),
			tg::Either::Right(resource) => resource.into_authorization_resource(),
		}
	}
}
