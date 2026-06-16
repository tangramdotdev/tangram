use {futures::FutureExt as _, std::time::Duration, tangram_client::prelude::*};

#[cfg(feature = "foundationdb")]
pub mod fdb;
#[cfg(feature = "lmdb")]
pub mod lmdb;

pub mod authorize;
pub mod batch;
pub mod cache;
pub mod clean;
pub mod grant;
pub mod group;
pub mod object;
pub mod organization;
pub mod process;
pub mod tag;
pub mod user;

pub mod prelude {
	pub use super::Index as _;
}

pub trait Index {
	fn authorize_batch(
		&self,
		args: &[crate::authorize::Arg],
		principal: Option<&tg::Principal>,
	) -> impl Future<Output = tg::Result<Vec<Option<crate::authorize::Output>>>> + Send;

	fn authorize(
		&self,
		resource: tg::grant::Resource,
		permissions: tg::grant::permission::Set,
		principal: Option<&tg::Principal>,
	) -> impl Future<Output = tg::Result<Option<crate::authorize::Output>>> + Send
	where
		Self: Sync,
	{
		let arg = crate::authorize::Arg {
			permissions,
			resource,
		};
		async move {
			self.authorize_batch(&[arg], principal)
				.await
				.map(|mut output| output.pop().unwrap())
		}
	}

	fn visible(
		&self,
		ids: &[tg::Id],
		principal: Option<&tg::Principal>,
	) -> impl Future<Output = tg::Result<Vec<bool>>> + Send;

	fn batch(&self, arg: crate::batch::Arg) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_cache_entries(
		&self,
		ids: &[tg::artifact::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::cache::Entry>>>> + Send;

	fn try_get_cache_entry(
		&self,
		id: &tg::artifact::Id,
	) -> impl Future<Output = tg::Result<Option<crate::cache::Entry>>> + Send {
		self.try_get_cache_entries(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_cache_entries(
		&self,
		ids: &[tg::artifact::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Vec<Option<crate::cache::Entry>>>> + Send;

	fn touch_cache_entry(
		&self,
		id: &tg::artifact::Id,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Option<crate::cache::Entry>>> + Send {
		self.touch_cache_entries(std::slice::from_ref(id), touched_at, time_to_touch)
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::object::Object>>>> + Send;

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<crate::object::Object>>> + Send {
		self.try_get_objects(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Vec<Option<crate::object::Object>>>> + Send;

	fn touch_object(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Option<crate::object::Object>>> + Send {
		self.touch_objects(std::slice::from_ref(id), touched_at, time_to_touch)
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::process::Process>>>> + Send;

	fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<crate::process::Process>>> + Send {
		self.try_get_processes(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Vec<Option<crate::process::Process>>>> + Send;

	fn touch_process(
		&self,
		id: &tg::process::Id,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Option<crate::process::Process>>> + Send {
		self.touch_processes(std::slice::from_ref(id), touched_at, time_to_touch)
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn put_grants(
		&self,
		args: &[crate::grant::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_grants(
		&self,
		args: &[crate::grant::delete::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_groups(
		&self,
		args: &[crate::group::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_groups(&self, ids: &[tg::group::Id]) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_group_members(
		&self,
		args: &[crate::group::member::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_group_members(
		&self,
		args: &[crate::group::member::delete::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_organizations(
		&self,
		args: &[crate::organization::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_organizations(
		&self,
		ids: &[tg::organization::Id],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_organization_members(
		&self,
		args: &[crate::organization::member::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_organization_members(
		&self,
		args: &[crate::organization::member::delete::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_tags(
		&self,
		args: &[crate::tag::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_tags(&self, ids: &[tg::tag::Id]) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_users(
		&self,
		args: &[crate::user::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_users(&self, ids: &[tg::user::Id]) -> impl Future<Output = tg::Result<()>> + Send;

	fn updates_finished(
		&self,
		transaction_id: u64,
	) -> impl Future<Output = tg::Result<bool>> + Send;

	fn update_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> impl Future<Output = tg::Result<usize>> + Send;

	fn clean(
		&self,
		now: i64,
		max_object_touched_at: i64,
		max_process_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> impl Future<Output = tg::Result<crate::clean::Output>> + Send;

	fn get_transaction_id(&self) -> impl Future<Output = tg::Result<u64>> + Send;

	fn sync(&self) -> impl Future<Output = tg::Result<()>> + Send;

	fn partition_total(&self) -> u64;
}
