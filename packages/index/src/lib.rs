use {futures::FutureExt as _, tangram_client::prelude::*};

pub mod batch;
pub mod checkout;
pub mod clean;
pub mod delegation;
pub mod group;
pub mod indexer;
pub mod object;
pub mod organization;
pub mod permission;
pub mod process;
#[doc(hidden)]
pub mod read;
pub mod sandbox;
pub mod tag;
pub mod update;
pub mod usage;
pub mod user;
pub mod verify;

pub mod prelude {
	pub use crate::{
		Index as _, checkout::Index as _, group::Index as _, indexer::Index as _,
		object::Index as _, organization::Index as _, permission::Index as _, process::Index as _,
		sandbox::Index as _, tag::Index as _, usage::Index as _, user::Index as _,
	};
}

pub trait Index:
	checkout::Index
	+ group::Index
	+ indexer::Index
	+ object::Index
	+ organization::Index
	+ permission::Index
	+ process::Index
	+ sandbox::Index
	+ tag::Index
	+ usage::Index
	+ user::Index
{
	fn verify_batch(
		&self,
		args: &[crate::verify::Arg],
		config: crate::verify::Config,
		principal: &tg::Principal,
	) -> impl Future<Output = tg::Result<Vec<crate::verify::Output>>> + Send;

	fn verify(
		&self,
		resource: tg::Selector<tg::Id>,
		permissions: tg::authorization::permission::Set,
		storage: tg::storage::Set,
		config: crate::verify::Config,
		principal: &tg::Principal,
	) -> impl Future<Output = tg::Result<crate::verify::Output>> + Send
	where
		Self: Sync,
	{
		let arg = crate::verify::Arg {
			subject: None,
			requested: permissions,
			required: permissions,
			resource,
			storage,
			tokens: Vec::new(),
		};
		async move {
			let mut outcomes = self.verify_batch(&[arg], config, principal).await?;
			let outcome = outcomes.pop().unwrap();

			Ok(outcome)
		}
	}

	fn contains_id(&self, id: &tg::Id) -> impl Future<Output = tg::Result<bool>> + Send {
		self.contains_ids(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn contains_ids(&self, ids: &[tg::Id]) -> impl Future<Output = tg::Result<Vec<bool>>> + Send;

	fn visible(
		&self,
		ids: &[tg::Id],
		principal: &tg::Principal,
	) -> impl Future<Output = tg::Result<Vec<bool>>> + Send;

	fn batch(
		&self,
		arg: crate::batch::Arg,
	) -> impl Future<Output = tg::Result<tg::Result<()>>> + Send;

	fn try_get_ancestors(
		&self,
		id: &tg::Id,
	) -> impl Future<Output = tg::Result<Option<Vec<tg::Id>>>> + Send;

	fn try_get_id_for_specifier(
		&self,
		specifier: &tg::Specifier,
	) -> impl Future<Output = tg::Result<Option<tg::Id>>> + Send {
		self.try_get_ids_for_specifiers(std::slice::from_ref(specifier))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn try_get_ids_for_specifiers(
		&self,
		specifiers: &[tg::Specifier],
	) -> impl Future<Output = tg::Result<Vec<Option<tg::Id>>>> + Send;

	fn get_requester_subjects(
		&self,
		principal: &tg::Principal,
	) -> impl Future<Output = tg::Result<Vec<tg::authorization::Subject>>> + Send;

	fn try_get_specifier_for_id(
		&self,
		id: &tg::Id,
	) -> impl Future<Output = tg::Result<Option<tg::Specifier>>> + Send {
		self.try_get_specifiers_for_ids(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn try_get_specifiers_for_ids(
		&self,
		ids: &[tg::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<tg::Specifier>>>> + Send;

	fn try_get_oldest_update_transaction_id(
		&self,
		kind: crate::update::Kind,
	) -> impl Future<Output = tg::Result<Option<u64>>> + Send;

	fn update_batch(
		&self,
		kind: crate::update::Kind,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> impl Future<Output = tg::Result<crate::update::Output>> + Send;

	fn clean(
		&self,
		arg: crate::clean::Arg,
	) -> impl Future<Output = tg::Result<crate::clean::Output>> + Send;

	fn get_transaction_id(&self) -> impl Future<Output = tg::Result<u64>> + Send;

	fn sync(&self) -> impl Future<Output = tg::Result<()>> + Send;

	#[must_use]
	fn cleaning_partition_total(&self) -> u64;

	#[must_use]
	fn permission_update_partition_total(&self) -> u64;

	#[must_use]
	fn storage_and_metadata_update_partition_total(&self) -> u64;

	#[must_use]
	fn usage_update_partition_total(&self) -> u64;
}
