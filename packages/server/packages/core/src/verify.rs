use {
	crate::{
		Session,
		authorization::{IntoAuthorizationResource, Outcome},
	},
	futures::future::BoxFuture,
	tangram_client::prelude::*,
};

pub struct Arg {
	pub permissions: tg::authorization::permission::Set,
	pub resource: tg::Selector<tg::Id>,
	pub storage: tg::storage::Set,
	pub tokens: Vec<tg::authorization::Token>,
	pub trusted: Option<tg::authorization::permission::Set>,
}

pub struct VerifyBatchArg<'a> {
	pub args: Vec<Arg>,
	pub current_sync: Option<&'a tg::sync::Id>,
	pub required: Option<tg::authorization::permission::Set>,
	pub subject: Option<tg::authorization::Subject>,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub expires_at: Option<i64>,
	pub outcome: Outcome,
	pub permissions: tg::authorization::permission::Set,
	pub storage: tg::storage::Set,
}

/// The verify component.
pub trait Component: crate::component::Lifecycle {
	fn verify_batch<'a>(
		&'a self,
		session: &'a Session,
		arg: VerifyBatchArg<'a>,
	) -> BoxFuture<'a, tg::Result<Vec<Output>>>;
}

#[must_use]
pub fn empty_storage(permissions: tg::authorization::permission::Set) -> tg::storage::Set {
	match permissions {
		tg::authorization::permission::Set::Process(_) => {
			tg::storage::Set::Process(tg::process::storage::Set::empty())
		},
		_ => tg::storage::Set::Object(tg::object::storage::Set::empty()),
	}
}

impl Output {
	pub fn check_exhaustion(self) -> tg::Result<Self> {
		if self.outcome == Outcome::Exhausted {
			return Err(tangram_index::verify::search_exhausted_error(
				"the verification search exhausted",
			));
		}
		Ok(self)
	}
}

impl Session {
	pub async fn verify(
		&self,
		resource: impl IntoAuthorizationResource,
		permissions: tg::authorization::permission::Set,
		storage: tg::storage::Set,
		current_sync: Option<&tg::sync::Id>,
	) -> tg::Result<Output> {
		let args = [(resource, permissions, None, storage)];
		let mut outputs = self.verify_batch_inner(args, None, current_sync).await?;
		Ok(outputs.pop().unwrap())
	}

	pub async fn verify_with_subject(
		&self,
		resource: impl IntoAuthorizationResource,
		requested: tg::authorization::permission::Set,
		required: tg::authorization::permission::Set,
		storage: tg::storage::Set,
		subject: tg::authorization::Subject,
	) -> tg::Result<Output> {
		let mut outputs = self
			.verify_batch_inner_with_subject(
				[(resource, requested, None, storage)],
				Some(required),
				Some(subject),
				None,
			)
			.await?;
		Ok(outputs.pop().unwrap())
	}

	pub async fn verify_batch<R, I>(&self, args: I) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set, tg::storage::Set)>,
	{
		let args = args.into_iter().collect::<Vec<_>>();

		let args = args
			.into_iter()
			.map(|(resource, permissions, storage)| (resource, permissions, None, storage));
		let outputs = self.verify_batch_inner(args, None, None).await?;
		Ok(outputs)
	}

	pub(crate) async fn verify_batch_with_required<R, I>(
		&self,
		args: I,
		required: tg::authorization::permission::Set,
		current_sync: Option<&tg::sync::Id>,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set, tg::storage::Set)>,
	{
		let args = args
			.into_iter()
			.map(|(resource, permissions, storage)| (resource, permissions, None, storage));
		self.verify_batch_inner(args, Some(required), current_sync)
			.await
	}

	pub async fn verify_batch_inner<R, I>(
		&self,
		args: I,
		required: Option<tg::authorization::permission::Set>,
		current_sync: Option<&tg::sync::Id>,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<
			Item = (
				R,
				tg::authorization::permission::Set,
				Option<tg::authorization::permission::Set>,
				tg::storage::Set,
			),
		>,
	{
		self.verify_batch_inner_with_subject(args, required, None, current_sync)
			.await
	}

	async fn verify_batch_inner_with_subject<R, I>(
		&self,
		args: I,
		required: Option<tg::authorization::permission::Set>,
		subject: Option<tg::authorization::Subject>,
		current_sync: Option<&tg::sync::Id>,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<
			Item = (
				R,
				tg::authorization::permission::Set,
				Option<tg::authorization::permission::Set>,
				tg::storage::Set,
			),
		>,
	{
		let args = args
			.into_iter()
			.map(|(resource, permissions, trusted, storage)| {
				let (resource, tokens) = resource.into_authorization_resource();
				Arg {
					permissions,
					resource,
					storage,
					tokens,
					trusted,
				}
			})
			.collect::<Vec<_>>();
		let arg = VerifyBatchArg {
			args,
			current_sync,
			required,
			subject,
		};
		self.server.components.verify.verify_batch(self, arg).await
	}
}
