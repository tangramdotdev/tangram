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
	) -> tg::Result<Option<tg::grant::Token>> {
		let Some(private_key) = self.server.tokens.private_key.as_ref() else {
			return Ok(None);
		};
		let body = tg::grant::Body {
			expires_at,
			permissions,
			resource,
		};
		let token = tg::grant::Token::sign(body, private_key)?;
		Ok(Some(token))
	}

	pub(crate) async fn authorize(
		&self,
		resource: impl IntoAuthorizationResource,
		permissions: impl Into<tg::grant::permission::Set>,
	) -> tg::Result<Option<tg::grant::permission::Set>> {
		let mut outputs = self
			.authorize_batch([(resource, permissions.into())])
			.await?;
		Ok(outputs.pop().unwrap())
	}

	pub(crate) async fn authorize_batch<R, I>(
		&self,
		args: I,
	) -> tg::Result<Vec<Option<tg::grant::permission::Set>>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::grant::permission::Set)>,
	{
		let mut outputs = Vec::new();
		let mut index_args = Vec::new();
		let mut index_positions = Vec::new();

		for (position, (resource, permissions)) in args.into_iter().enumerate() {
			let (resource, token) = resource.into_authorization_resource();
			let token = if let Some(token) = token {
				// Authorize an exact token if there is one.
				if self.authorize_token(&resource, permissions, &token) {
					outputs.push(Some(permissions));
					continue;
				}
				self.verify_token(&token).then_some(token.body)
			} else {
				None
			};

			// Authorize the root principal for all resources.
			if matches!(self.context.principal, tg::Principal::Root) {
				outputs.push(Some(permissions));
				continue;
			}

			// Authorize a sandbox for its own processes.
			if let (
				tg::grant::Resource::Id(id),
				tg::grant::permission::Set::Process(_),
				tg::Principal::Sandbox(sandbox),
			) = (&resource, permissions, &self.context.principal)
				&& let Ok(process) = tg::process::Id::try_from(id.clone())
				&& let Some(output) = self.server.try_get_process_local(&process, false).await?
				&& output.data.sandbox == *sandbox
			{
				outputs.push(Some(permissions));
				continue;
			}

			outputs.push(None);
			index_positions.push(position);
			index_args.push(tangram_index::authorize::Arg {
				permissions,
				resource,
				token,
			});
		}

		// Attempt to authorize.
		let index_outputs = self
			.server
			.index
			.authorize_batch(&index_args, &self.context.principal)
			.await?;
		for (position, output) in std::iter::zip(&index_positions, index_outputs) {
			if let Some(output) = output
				&& !output.permissions.is_empty()
			{
				outputs[*position] = Some(output.permissions);
			}
		}
		if index_args.is_empty() || outputs.iter().all(Option::is_some) {
			return Ok(outputs);
		}

		// Index.
		self.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;

		// Attempt to authorize again.
		let index_outputs = self
			.server
			.index
			.authorize_batch(&index_args, &self.context.principal)
			.await?;
		for (position, output) in std::iter::zip(index_positions, index_outputs) {
			if let Some(output) = output {
				outputs[position] = Some(output.permissions);
			}
		}

		Ok(outputs)
	}

	pub(crate) async fn authorize_owner(&self, owner: Option<&tg::Principal>) -> tg::Result<()> {
		let Some(owner) = owner else {
			return Ok(());
		};
		let authorized = match owner.to_id() {
			Some(id) => {
				let permission = Self::write_permission_for_resource(&id)?;
				self.authorize(tg::grant::Resource::Id(id), permission)
					.await?
					.is_some_and(|permissions| permissions.contains(permission))
			},
			None => matches!(self.context.principal, tg::Principal::Root),
		};
		if !authorized {
			return Err(tg::error!("unauthorized"));
		}
		Ok(())
	}

	pub(crate) fn authorize_token(
		&self,
		resource: &tg::grant::Resource,
		permissions: tg::grant::permission::Set,
		token: &tg::grant::Token,
	) -> bool {
		if token.body.resource != *resource {
			return false;
		}
		if !self.verify_token(token) {
			return false;
		}
		permissions
			.iter()
			.all(|permission| token.body.grants(permission))
	}

	fn verify_token(&self, token: &tg::grant::Token) -> bool {
		let Some(public_key) = self.server.tokens.public_keys.get(&token.metadata.key) else {
			return false;
		};
		if token.verify(public_key).is_err() {
			return false;
		}
		true
	}
}

pub(crate) trait IntoResource {
	fn into_resource(self) -> tg::grant::Resource;
}

pub(crate) trait IntoAuthorizationResource {
	fn into_authorization_resource(self) -> (tg::grant::Resource, Option<tg::grant::Token>);
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

impl IntoResource for tg::sandbox::Id {
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
	fn into_authorization_resource(self) -> (tg::grant::Resource, Option<tg::grant::Token>) {
		(self.into_resource(), None)
	}
}

impl<T> IntoAuthorizationResource for tg::WithToken<T>
where
	T: IntoResource,
{
	fn into_authorization_resource(self) -> (tg::grant::Resource, Option<tg::grant::Token>) {
		(self.id.into_resource(), Some(self.token))
	}
}

impl<T> IntoAuthorizationResource for tg::MaybeWithToken<T>
where
	T: IntoResource,
{
	fn into_authorization_resource(self) -> (tg::grant::Resource, Option<tg::grant::Token>) {
		match self {
			tg::Either::Left(resource) => (resource.into_resource(), None),
			tg::Either::Right(resource) => resource.into_authorization_resource(),
		}
	}
}
