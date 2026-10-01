use {crate::Session, std::collections::BTreeMap, tangram_client::prelude::*};

#[cfg(test)]
mod tests;
mod token;

pub(crate) use tangram_index::verify::Outcome;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Output {
	pub expires_at: Option<i64>,
	pub outcome: Outcome,
	pub permissions: tg::authorization::permission::Set,
}

#[derive(Default)]
pub(crate) struct Proofs {
	known: bool,
	permissions: BTreeMap<tg::authorization::Permission, Option<i64>>,
}

pub(crate) fn check_exhaustion(outputs: &[Output]) -> tg::Result<()> {
	for output in outputs {
		output.check_exhaustion()?;
	}
	Ok(())
}

impl Output {
	pub(crate) fn check_exhaustion(self) -> tg::Result<Self> {
		if self.outcome == Outcome::Exhausted {
			return Err(tangram_index::verify::search_exhausted_error(
				"the authorization search exhausted",
			));
		}
		Ok(self)
	}

	pub(crate) fn into_result(self) -> tg::Result<Self> {
		match self.outcome {
			Outcome::Exhausted => Err(tangram_index::verify::search_exhausted_error(
				"the authorization search exhausted",
			)),
			Outcome::Satisfied => Ok(self),
			Outcome::Unsatisfied => Err(tg::error!("unauthorized")),
		}
	}
}

impl Proofs {
	pub(crate) fn insert(&mut self, requested: tg::authorization::permission::Set, output: Output) {
		if output.permissions.kind() != requested.kind() {
			return;
		}
		self.known = true;
		for permission in requested.iter().filter(|permission| {
			output
				.permissions
				.iter()
				.any(|proof| proof.implies(*permission))
		}) {
			self.permissions
				.entry(permission)
				.and_modify(|expires_at| {
					*expires_at = match (*expires_at, output.expires_at) {
						(Some(left), Some(right)) => Some(left.max(right)),
						_ => None,
					};
				})
				.or_insert(output.expires_at);
		}
	}

	pub(crate) fn output(&self, requested: tg::authorization::permission::Set) -> Option<Output> {
		let mut permissions = requested.empty_like();
		let mut expires_at = None;
		for (permission, expiration) in &self.permissions {
			permissions.insert(tg::authorization::permission::Set::from_permission(
				*permission,
			));
			if let Some(expiration) = expiration {
				expires_at =
					Some(expires_at.map_or(*expiration, |value: i64| value.min(*expiration)));
			}
		}
		self.known.then_some(Output {
			expires_at,
			outcome: if permissions.contains(requested) {
				Outcome::Satisfied
			} else {
				Outcome::Unsatisfied
			},
			permissions,
		})
	}
}

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
	) -> tg::Result<Output> {
		let mut outputs = self
			.authorize_batch([(resource, permissions.into())])
			.await?;
		Ok(outputs.pop().unwrap())
	}

	pub(crate) async fn authorize_batch<R, I>(&self, args: I) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		let args = args
			.into_iter()
			.map(|(resource, permissions)| (resource, permissions, None));
		let outputs = self.authorize_batch_inner(args, None, false).await?;
		let outputs = outputs
			.into_iter()
			.map(|output| Output {
				expires_at: output.expires_at,
				outcome: output.outcome,
				permissions: output.permissions,
			})
			.collect();
		Ok(outputs)
	}

	pub(crate) async fn authorize_batch_initial<R, I>(
		&self,
		args: I,
		required: tg::authorization::permission::Set,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		let outputs = self.verify_batch_initial(args, required).await?;
		let outputs = outputs
			.into_iter()
			.map(|output| Output {
				expires_at: output.expires_at,
				outcome: output.outcome,
				permissions: output.permissions,
			})
			.collect();
		Ok(outputs)
	}

	pub(crate) async fn authorize_batch_with_required<R, I>(
		&self,
		args: I,
		required: tg::authorization::permission::Set,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		let args = args.into_iter().map(|(resource, permissions)| {
			(
				resource,
				permissions,
				crate::verify::empty_storage(permissions),
			)
		});
		let outputs = self.verify_batch_with_required(args, required).await?;
		let outputs = outputs
			.into_iter()
			.map(|output| Output {
				expires_at: output.expires_at,
				outcome: output.outcome,
				permissions: output.permissions,
			})
			.collect();
		Ok(outputs)
	}

	pub(crate) async fn authorize_object_read(
		&self,
		resource: impl IntoAuthorizationResource,
		wait_for_subtree: bool,
	) -> tg::Result<Output> {
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
	) -> tg::Result<Vec<Output>>
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
		let args = resources
			.into_iter()
			.map(|resource| (resource, requested, None));

		self.authorize_batch_inner(args, Some(required.into()), wait_for_subtree)
			.await
	}

	pub(crate) async fn authorize_with_permissions(
		&self,
		resource: impl IntoAuthorizationResource,
		requested: tg::authorization::permission::Set,
		required: tg::authorization::permission::Set,
		proven: tg::authorization::permission::Set,
	) -> tg::Result<Output> {
		let args = [(resource, requested, Some(proven))];
		let mut outputs = self
			.authorize_batch_inner(args, Some(required), false)
			.await?;
		Ok(outputs.pop().unwrap())
	}

	async fn authorize_batch_inner<R, I>(
		&self,
		args: I,
		required: Option<tg::authorization::permission::Set>,
		wait_for_requested_permissions: bool,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<
			Item = (
				R,
				tg::authorization::permission::Set,
				Option<tg::authorization::permission::Set>,
			),
		>,
	{
		let args = args.into_iter().map(|(resource, permissions, trusted)| {
			(
				resource,
				permissions,
				trusted,
				crate::verify::empty_storage(permissions),
			)
		});
		let outputs = self
			.verify_batch_inner(args, required, wait_for_requested_permissions)
			.await?;
		let outputs = outputs
			.into_iter()
			.map(|output| Output {
				expires_at: output.expires_at,
				outcome: output.outcome,
				permissions: output.permissions,
			})
			.collect();
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
					.check_exhaustion()?
					.permissions
					.contains(permission)
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
			.all(|permission| token.body.authorizes(permission))
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
	fn into_authorization_resource(self) -> (tg::Selector<tg::Id>, Vec<tg::authorization::Token>);
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
	fn into_authorization_resource(self) -> (tg::Selector<tg::Id>, Vec<tg::authorization::Token>) {
		(self.into_resource(), Vec::new())
	}
}

impl<T> IntoAuthorizationResource for tg::Referent<T>
where
	T: IntoResource,
{
	fn into_authorization_resource(self) -> (tg::Selector<tg::Id>, Vec<tg::authorization::Token>) {
		(
			self.node.into_resource(),
			self.options.tokens.local_authorization().to_vec(),
		)
	}
}
