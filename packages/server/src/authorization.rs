use {
	crate::Session, futures::FutureExt as _, tangram_client::prelude::*,
	tangram_futures::stream::TryExt, tangram_index::prelude::*,
};

mod token;

impl Session {
	pub(crate) fn create_token(
		&self,
		resource: tg::Id,
		permissions: Vec<tg::authorization::Permission>,
		expires_at: i64,
	) -> tg::Result<Option<tg::authorization::Token>> {
		let Some(private_key) = self.server.authorization_tokens.private_key.as_ref() else {
			return Ok(None);
		};
		let body = tg::authorization::Body {
			expires_at,
			permissions,
			resource,
		};
		let token = tg::authorization::Token::sign(body, private_key)?;
		Ok(Some(token))
	}

	pub(crate) async fn authorize(
		&self,
		resource: impl IntoAuthorizationResource,
		permissions: impl Into<tg::authorization::permission::Set>,
	) -> tg::Result<Option<tg::authorization::permission::Set>> {
		let mut outputs = self
			.authorize_batch([(resource, permissions.into())])
			.await?;
		Ok(outputs.pop().unwrap())
	}

	pub(crate) async fn authorize_batch<R, I>(
		&self,
		args: I,
	) -> tg::Result<Vec<Option<tg::authorization::permission::Set>>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		self.authorize_batch_inner(args, None).await
	}

	pub(crate) async fn authorize_batch_with_required<R, I>(
		&self,
		args: I,
		required: tg::authorization::permission::Set,
	) -> tg::Result<Vec<Option<tg::authorization::permission::Set>>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		self.authorize_batch_inner(args, Some(required)).await
	}

	async fn authorize_batch_inner<R, I>(
		&self,
		args: I,
		required: Option<tg::authorization::permission::Set>,
	) -> tg::Result<Vec<Option<tg::authorization::permission::Set>>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		let mut outputs = Vec::new();
		let mut index_args = Vec::new();
		let mut index_positions = Vec::new();
		let mut index_required = Vec::new();

		for (position, (resource, permissions)) in args.into_iter().enumerate() {
			let required = required.unwrap_or(permissions);
			if !permissions.contains(required) {
				return Err(tg::error!(
					"the required permissions must be contained in the requested permissions"
				));
			}
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
				tg::Selector::Id(id),
				tg::authorization::permission::Set::Process(_),
				tg::Principal::Sandbox(sandbox),
			) = (&resource, permissions, &self.context.principal)
				&& let Ok(process) = tg::process::Id::try_from(id.clone())
				&& let Some(output) = self
					.try_get_process_local_inner(&process, false)
					.boxed()
					.await? && output.data.sandbox == *sandbox
			{
				outputs.push(Some(permissions));
				continue;
			}

			outputs.push(None);
			index_positions.push(position);
			index_required.push(required);
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
		// Refresh the index unless every required permission was already granted.
		let needs_index =
			std::iter::zip(&index_outputs, &index_required).any(|(output, required)| {
				output
					.as_ref()
					.is_none_or(|output| !output.permissions.contains(*required))
			});
		for (position, output) in std::iter::zip(&index_positions, index_outputs) {
			if let Some(output) = output {
				outputs[*position] = Some(output.permissions);
			}
		}
		if !needs_index {
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
				self.authorize(tg::Selector::Id(id), permission)
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
		resource: &tg::Selector<tg::Id>,
		permissions: tg::authorization::permission::Set,
		token: &tg::authorization::Token,
	) -> bool {
		if !matches!(resource, tg::Selector::Id(id) if token.body.resource == *id) {
			return false;
		}
		if !self.verify_token(token) {
			return false;
		}
		permissions
			.iter()
			.all(|permission| token.body.grants(permission))
	}

	pub(crate) fn verify_local_token(&self, token: &tg::authorization::Token) -> bool {
		self.server
			.authorization_tokens
			.private_key
			.as_ref()
			.is_some_and(|private_key| private_key.name == token.metadata.key)
			&& self.verify_token(token)
	}

	pub(crate) fn verify_token(&self, token: &tg::authorization::Token) -> bool {
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
		if token.verify_at(public_key, now).is_err() {
			return false;
		}
		true
	}
}

pub(crate) trait IntoResource {
	fn into_resource(self) -> tg::Selector<tg::Id>;
}

pub(crate) trait IntoAuthorizationResource {
	fn into_authorization_resource(
		self,
	) -> (tg::Selector<tg::Id>, Option<tg::authorization::Token>);
}

impl IntoResource for tg::Id {
	fn into_resource(self) -> tg::Selector<tg::Id> {
		tg::Selector::Id(self)
	}
}

impl IntoResource for tg::object::Id {
	fn into_resource(self) -> tg::Selector<tg::Id> {
		tg::Selector::Id(self.into())
	}
}

impl IntoResource for tg::process::Id {
	fn into_resource(self) -> tg::Selector<tg::Id> {
		tg::Selector::Id(self.into())
	}
}

impl IntoResource for tg::sandbox::Id {
	fn into_resource(self) -> tg::Selector<tg::Id> {
		tg::Selector::Id(self.into())
	}
}

impl IntoResource for tg::artifact::Id {
	fn into_resource(self) -> tg::Selector<tg::Id> {
		tg::Selector::Id(tg::object::Id::from(self).into())
	}
}

impl<I> IntoResource for tg::Selector<I>
where
	I: Into<tg::Id>,
{
	fn into_resource(self) -> tg::Selector<tg::Id> {
		match self {
			tg::Selector::Id(id) => tg::Selector::Id(id.into()),
			tg::Selector::Specifier(specifier) => tg::Selector::Specifier(specifier),
		}
	}
}

impl<T> IntoAuthorizationResource for T
where
	T: IntoResource,
{
	fn into_authorization_resource(
		self,
	) -> (tg::Selector<tg::Id>, Option<tg::authorization::Token>) {
		(self.into_resource(), None)
	}
}

impl<T> IntoAuthorizationResource for tg::Referent<T>
where
	T: IntoResource,
{
	fn into_authorization_resource(
		self,
	) -> (tg::Selector<tg::Id>, Option<tg::authorization::Token>) {
		(
			self.node.into_resource(),
			self.options.tokens.local().cloned(),
		)
	}
}
