use {
	crate::Session, futures::FutureExt as _, tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _, tangram_index::prelude::*,
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
		self.authorize_batch_inner(args, None, false).await
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
		self.authorize_batch_inner(args, Some(required), false)
			.await
	}

	pub(crate) async fn authorize_object_read(
		&self,
		resource: impl IntoAuthorizationResource,
		wait_for_subtree: bool,
	) -> tg::Result<Option<tg::authorization::permission::Set>> {
		let mut outputs = self
			.authorize_object_read_batch([resource], wait_for_subtree)
			.await?;
		let output = outputs.pop().unwrap();

		Ok(output)
	}

	pub(crate) async fn authorize_object_read_batch<R, I>(
		&self,
		resources: I,
		wait_for_subtree: bool,
	) -> tg::Result<Vec<Option<tg::authorization::permission::Set>>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = R>,
	{
		// Request the optional subtree permission while requiring the node permission.
		let mut requested = tg::authorization::permission::object::Set::empty();
		requested.insert(tg::authorization::permission::object::Set::NODE);
		requested.insert(tg::authorization::permission::object::Set::SUBTREE);
		let requested = tg::authorization::permission::Set::Object(requested);
		let required = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let args = resources.into_iter().map(|resource| (resource, requested));

		self.authorize_batch_inner(args, Some(required.into()), wait_for_subtree)
			.await
	}

	async fn authorize_batch_inner<R, I>(
		&self,
		args: I,
		required: Option<tg::authorization::permission::Set>,
		wait_for_requested_permissions: bool,
	) -> tg::Result<Vec<Option<tg::authorization::permission::Set>>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		let mut outputs = Vec::new();
		let mut index_args = Vec::new();
		let mut index_positions = Vec::new();

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
				if self.verify_token(&token) {
					if self
						.authorize_object_child_token(&resource, permissions, &token.body)
						.await?
					{
						outputs.push(Some(permissions));
						continue;
					}
					Some(token.body)
				} else {
					None
				}
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
			index_args.push(tangram_index::authorize::Arg {
				required,
				requested: permissions,
				resource,
				token,
			});
		}

		if index_args.is_empty() {
			return Ok(outputs);
		}
		for arg in &index_args {
			let token_resource = arg
				.token
				.as_ref()
				.map(|body| body.resource.to_string())
				.unwrap_or_default();
			crate::checkpoint!(
				self.server,
				"authorization.index",
				resource = %arg.resource,
				token_resource,
			)
			.await;
		}

		// Run at most one authorization search at a time while indexing catches up.
		let authorization = &self.server.config.authorization;
		let delay = authorization.index.delay;
		let initial_config = crate::authorization_search_config(&authorization.initial);
		let initial_is_sufficient = |outcomes: &[tangram_index::authorize::Outcome]| {
			outcomes.len() == index_args.len()
				&& std::iter::zip(outcomes, &index_args).all(|(outcome, arg)| {
					let permissions = if wait_for_requested_permissions {
						arg.requested
					} else {
						arg.required
					};
					outcome
						.output()
						.is_some_and(|output| output.permissions.contains(permissions))
				})
		};
		let initial =
			self.server
				.index
				.authorize_batch(&index_args, initial_config, &self.context.principal);
		tokio::pin!(initial);
		let initial_result = match delay {
			Some(delay) => tokio::select! {
				result = &mut initial => Some(result),
				() = tokio::time::sleep(delay) => None,
			},
			None => Some((&mut initial).await),
		};
		let index_wait = async {
			self.index()
				.await
				.map_err(|error| tg::error!(!error, "failed to index"))?
				.try_last()
				.await
				.map_err(|error| tg::error!(!error, "failed to index"))?;

			Ok::<_, tg::Error>(())
		};
		let final_config = crate::authorization_search_config(&authorization.final_);
		let final_authorization = || async {
			let outcomes = self
				.server
				.index
				.authorize_batch(&index_args, final_config, &self.context.principal)
				.await?;

			ensure_authorization_search_complete(outcomes)
		};
		let index_outcomes = match initial_result {
			Some(Ok(outcomes)) if initial_is_sufficient(&outcomes) => outcomes,
			Some(Ok(_)) => {
				index_wait.await?;
				final_authorization().await?
			},
			Some(Err(error)) => return Err(error),
			None => {
				tokio::pin!(index_wait);
				tokio::select! {
					result = &mut initial => match result {
						Ok(outcomes) if initial_is_sufficient(&outcomes) => outcomes,
						Ok(_) => {
							index_wait.await?;
							final_authorization().await?
						},
						Err(error) => return Err(error),
					},
					result = &mut index_wait => match result {
						Ok(()) => match initial.await {
							Ok(outcomes) if initial_is_sufficient(&outcomes) => outcomes,
							Ok(_) | Err(_) => final_authorization().await?,
						},
						Err(error) => match initial.await {
							Ok(outcomes) if initial_is_sufficient(&outcomes) => outcomes,
							Ok(_) | Err(_) => return Err(error),
						},
					},
				}
			},
		};
		for (position, outcome) in std::iter::zip(index_positions, index_outcomes) {
			let output = match outcome {
				tangram_index::authorize::Outcome::Authorized(output) => Some(output),
				tangram_index::authorize::Outcome::Denied(output) => output,
				outcome @ tangram_index::authorize::Outcome::Exhausted => {
					Some(outcome.into_result()?)
				},
			};
			if let Some(output) = output {
				outputs[position] = Some(output.permissions);
			}
		}

		Ok(outputs)
	}

	async fn authorize_object_child_token(
		&self,
		resource: &tg::Selector<tg::Id>,
		permissions: tg::authorization::permission::Set,
		token: &tg::authorization::Body,
	) -> tg::Result<bool> {
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		if !permissions
			.iter()
			.all(|permission| subtree.implies(permission))
			|| !token.grants(subtree)
		{
			return Ok(false);
		}
		let Ok(parent) = tg::object::Id::try_from(token.resource.clone()) else {
			return Ok(false);
		};
		let tg::Selector::Id(resource) = resource else {
			return Ok(false);
		};
		let Ok(child) = tg::object::Id::try_from(resource.clone()) else {
			return Ok(false);
		};
		let Some(children) = self.server.index.try_get_object_children(&parent).await? else {
			return Ok(false);
		};
		let authorized = children.contains(&child);

		Ok(authorized)
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

fn ensure_authorization_search_complete(
	outcomes: Vec<tangram_index::authorize::Outcome>,
) -> tg::Result<Vec<tangram_index::authorize::Outcome>> {
	if outcomes
		.iter()
		.any(|outcome| matches!(outcome, tangram_index::authorize::Outcome::Exhausted))
	{
		tangram_index::authorize::Outcome::Exhausted.into_result()?;
	}

	Ok(outcomes)
}
