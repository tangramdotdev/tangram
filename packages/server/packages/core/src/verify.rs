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
	pub mode: Mode,
	pub required: Option<tg::authorization::permission::Set>,
	pub subject: Option<tg::authorization::Subject>,
	pub wait_for_requested_permissions: bool,
}

pub enum Mode {
	Initial,
	Normal,
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

#[must_use]
pub fn permission_storage(permission: tg::authorization::Permission) -> Option<tg::storage::Set> {
	match permission {
		tg::authorization::Permission::Object(permission) => {
			let storage = match permission {
				tg::authorization::permission::object::Permission::Node => {
					tg::object::storage::Storage::Node
				},
				tg::authorization::permission::object::Permission::Subtree => {
					tg::object::storage::Storage::Subtree
				},
			};
			Some(tg::storage::Set::Object(
				tg::object::storage::Set::from_storage(storage),
			))
		},
		tg::authorization::Permission::Process(permission) => {
			let storage = match permission {
				tg::authorization::permission::process::Permission::Node => {
					tg::process::storage::Storage::Node
				},
				tg::authorization::permission::process::Permission::NodeCommandObjects => {
					tg::process::storage::Storage::NodeCommandObjects
				},
				tg::authorization::permission::process::Permission::NodeErrorObjects => {
					tg::process::storage::Storage::NodeErrorObjects
				},
				tg::authorization::permission::process::Permission::NodeLogObjects => {
					tg::process::storage::Storage::NodeLogObjects
				},
				tg::authorization::permission::process::Permission::NodeOutputObjects => {
					tg::process::storage::Storage::NodeOutputObjects
				},
				tg::authorization::permission::process::Permission::Subtree => {
					tg::process::storage::Storage::Subtree
				},
				tg::authorization::permission::process::Permission::SubtreeCommandObjects => {
					tg::process::storage::Storage::SubtreeCommandObjects
				},
				tg::authorization::permission::process::Permission::SubtreeErrorObjects => {
					tg::process::storage::Storage::SubtreeErrorObjects
				},
				tg::authorization::permission::process::Permission::SubtreeLogObjects => {
					tg::process::storage::Storage::SubtreeLogObjects
				},
				tg::authorization::permission::process::Permission::SubtreeOutputObjects => {
					tg::process::storage::Storage::SubtreeOutputObjects
				},
				tg::authorization::permission::process::Permission::Parent => return None,
			};
			Some(tg::storage::Set::Process(
				tg::process::storage::Set::from_storage(storage),
			))
		},
		_ => None,
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
		let mut outputs = self
			.verify_batch_inner(args, None, false, current_sync)
			.await?;
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
				true,
				Some(subject),
				Mode::Normal,
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
		let outputs = self.verify_batch_inner(args, None, false, None).await?;
		Ok(outputs)
	}

	pub(crate) async fn verify_batch_initial<R, I>(
		&self,
		args: I,
		required: tg::authorization::permission::Set,
		current_sync: Option<&tg::sync::Id>,
	) -> tg::Result<Vec<Output>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		let args = args
			.into_iter()
			.map(|(resource, requested)| (resource, requested, None, empty_storage(requested)));
		self.verify_batch_inner_with_subject(
			args,
			Some(required),
			false,
			None,
			Mode::Initial,
			current_sync,
		)
		.await
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
		self.verify_batch_inner(args, Some(required), false, current_sync)
			.await
	}

	pub async fn verify_batch_inner<R, I>(
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
				tg::storage::Set,
			),
		>,
	{
		self.verify_batch_inner_with_subject(
			args,
			required,
			wait_for_requested_permissions,
			None,
			Mode::Normal,
			current_sync,
		)
		.await
	}

	async fn verify_batch_inner_with_subject<R, I>(
		&self,
		args: I,
		required: Option<tg::authorization::permission::Set>,
		wait_for_requested_permissions: bool,
		subject: Option<tg::authorization::Subject>,
		mode: Mode,
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
			mode,
			required,
			subject,
			wait_for_requested_permissions,
		};
		self.server.components.verify.verify_batch(self, arg).await
	}
}
