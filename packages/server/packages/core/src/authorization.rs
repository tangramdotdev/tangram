use {
	crate::Session, futures::future::BoxFuture, std::collections::BTreeMap,
	tangram_client::prelude::*,
};

#[cfg(test)]
mod tests;
mod token;

pub use tangram_index::verify::Outcome;

/// The authorization component.
pub trait Component: crate::component::Lifecycle {
	fn add_permanent_token_to_object_reference<'a>(
		&'a self,
		session: &'a Session,
		reference: &'a mut tg::Reference,
		resource: &'a tg::artifact::Id,
	) -> tg::Result<()>;

	fn add_tokens_to_module_data_with_expires_at<'a>(
		&'a self,
		session: &'a Session,
		module: &'a mut tg::module::Data,
		expires_at: i64,
	) -> tg::Result<()>;

	fn add_tokens_to_value_data<'a>(
		&'a self,
		session: &'a Session,
		data: &'a mut tg::value::Data,
	) -> tg::Result<()>;

	fn add_tokens_to_value_data_with_expires_at<'a>(
		&'a self,
		session: &'a Session,
		data: &'a mut tg::value::Data,
		expires_at: i64,
	) -> tg::Result<()>;

	fn authorize_owner<'a>(
		&'a self,
		session: &'a Session,
		owner: Option<&'a tg::Principal>,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn authorize_token<'a>(
		&'a self,
		session: &'a Session,
		resource: &'a tg::Selector<tg::Id>,
		permissions: tg::authorization::permission::Set,
		token: &'a tg::authorization::Token,
	) -> bool;

	fn create_permanent_object_token<'a>(
		&'a self,
		session: &'a Session,
		resource: &'a tg::artifact::Id,
	) -> tg::Result<Option<tg::authorization::Token>>;

	fn create_read_token<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::Id,
	) -> tg::Result<Option<tg::authorization::Token>>;

	fn create_token<'a>(
		&'a self,
		session: &'a Session,
		resource: tg::Id,
		permissions: Vec<tg::authorization::Permission>,
		expires_at: i64,
	) -> tg::Result<Option<tg::authorization::Token>>;

	fn try_get_sync_id_from_token<'a>(
		&'a self,
		session: &'a Session,
		token: &'a tg::authorization::Token,
	) -> Option<tg::sync::Id>;

	fn update_error_data_referents_for_location<'a>(
		&'a self,
		session: &'a Session,
		data: &'a mut tg::error::Data,
		location: &'a tg::Location,
		trusted: bool,
	) -> tg::Result<()>;

	fn update_process_child_referent_for_location<'a>(
		&'a self,
		session: &'a Session,
		child: &'a mut tg::process::data::Child,
		location: &'a tg::Location,
		trusted: bool,
	) -> tg::Result<()>;

	fn update_process_data_referents_for_location<'a>(
		&'a self,
		session: &'a Session,
		data: &'a mut tg::process::Data,
		location: &'a tg::Location,
		trusted: bool,
	) -> tg::Result<()>;

	fn update_tokens_and_location<'a>(
		&'a self,
		session: &'a Session,
		tokens: &'a mut tg::authorization::Tokens,
		output_location: Option<&'a mut Option<tg::Location>>,
		location: &'a tg::Location,
		trusted: bool,
	) -> tg::Result<()>;

	fn update_value_data_referents_for_location<'a>(
		&'a self,
		session: &'a Session,
		data: &'a mut tg::value::Data,
		location: &'a tg::Location,
		trusted: bool,
	) -> tg::Result<()>;

	fn verify_local_token<'a>(
		&'a self,
		session: &'a Session,
		token: &'a tg::authorization::Token,
	) -> bool;

	fn verify_token<'a>(
		&'a self,
		session: &'a Session,
		token: &'a tg::authorization::Token,
	) -> bool;
}

#[derive(Clone, Copy, Debug)]
pub struct Output {
	pub expires_at: Option<i64>,
	pub outcome: Outcome,
	pub permissions: tg::authorization::permission::Set,
}

#[derive(Default)]
pub struct Proofs {
	known: bool,
	permissions: BTreeMap<tg::authorization::Permission, Option<i64>>,
}

pub(crate) trait IntoResource {
	fn into_resource(self) -> tg::Selector<tg::Id>;
}

pub trait IntoAuthorizationResource {
	fn into_authorization_resource(self) -> (tg::Selector<tg::Id>, Vec<tg::authorization::Token>);
}

pub fn check_exhaustion(outputs: &[Output]) -> tg::Result<()> {
	for output in outputs {
		output.check_exhaustion()?;
	}
	Ok(())
}

impl Output {
	pub fn check_exhaustion(self) -> tg::Result<Self> {
		if self.outcome == Outcome::Exhausted {
			return Err(tangram_index::verify::search_exhausted_error(
				"the authorization search exhausted",
			));
		}
		Ok(self)
	}

	pub fn into_result(self) -> tg::Result<Self> {
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
	pub fn insert(&mut self, requested: tg::authorization::permission::Set, output: Output) {
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

	#[must_use]
	pub fn output(&self, requested: tg::authorization::permission::Set) -> Option<Output> {
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
	pub async fn authorize(
		&self,
		resource: impl IntoAuthorizationResource,
		permissions: impl Into<tg::authorization::permission::Set>,
	) -> tg::Result<Output> {
		let mut outputs = self
			.authorize_batch([(resource, permissions.into())], None)
			.await?;
		Ok(outputs.pop().unwrap())
	}

	pub async fn authorize_batch<R, I>(
		&self,
		args: I,
		current_sync: Option<&tg::sync::Id>,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		let args = args
			.into_iter()
			.map(|(resource, permissions)| (resource, permissions, None));
		let outputs = self
			.authorize_batch_inner(args, None, false, current_sync)
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

	pub async fn authorize_batch_initial<R, I>(
		&self,
		args: I,
		required: tg::authorization::permission::Set,
		current_sync: Option<&tg::sync::Id>,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		let outputs = self
			.verify_batch_initial(args, required, current_sync)
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

	pub async fn authorize_batch_with_required<R, I>(
		&self,
		args: I,
		required: tg::authorization::permission::Set,
		current_sync: Option<&tg::sync::Id>,
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
		let outputs = self
			.verify_batch_with_required(args, required, current_sync)
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

	pub async fn authorize_object_read(
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

		self.authorize_batch_inner(args, Some(required.into()), wait_for_subtree, None)
			.await
	}

	pub async fn authorize_with_permissions(
		&self,
		resource: impl IntoAuthorizationResource,
		requested: tg::authorization::permission::Set,
		required: tg::authorization::permission::Set,
		proven: tg::authorization::permission::Set,
		current_sync: Option<&tg::sync::Id>,
	) -> tg::Result<Output> {
		let args = [(resource, requested, Some(proven))];
		let mut outputs = self
			.authorize_batch_inner(args, Some(required), false, current_sync)
			.await?;
		Ok(outputs.pop().unwrap())
	}

	async fn authorize_batch_inner<R, I>(
		&self,
		args: I,
		required: Option<tg::authorization::permission::Set>,
		wait_for_requested_permissions: bool,
		current_sync: Option<&tg::sync::Id>,
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
			.verify_batch_inner(args, required, wait_for_requested_permissions, current_sync)
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
