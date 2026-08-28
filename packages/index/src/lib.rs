use {futures::FutureExt as _, std::time::Duration, tangram_client::prelude::*};

#[cfg(feature = "foundationdb")]
pub mod fdb;
#[cfg(feature = "lmdb")]
pub mod lmdb;

pub mod authorize;
pub mod batch;
pub mod checkout;
pub mod clean;
pub mod grant;
pub mod group;
pub mod log;
pub mod object;
pub mod organization;
pub mod process;
mod read;
pub mod sandbox;
pub mod tag;
pub mod update;
pub mod usage;
pub mod user;

pub mod prelude {
	pub use super::Index as _;
}

pub trait Index {
	fn authorize_batch(
		&self,
		args: &[crate::authorize::Arg],
		config: crate::authorize::Config,
		principal: &tg::Principal,
	) -> impl Future<Output = tg::Result<Vec<crate::authorize::Outcome>>> + Send;

	fn authorize(
		&self,
		resource: tg::Selector<tg::Id>,
		permissions: tg::authorization::permission::Set,
		config: crate::authorize::Config,
		principal: &tg::Principal,
	) -> impl Future<Output = tg::Result<crate::authorize::Outcome>> + Send
	where
		Self: Sync,
	{
		let arg = crate::authorize::Arg {
			required: permissions,
			requested: permissions,
			resource,
			token: None,
		};
		async move {
			let mut outcomes = self.authorize_batch(&[arg], config, principal).await?;
			let outcome = outcomes.pop().unwrap();

			Ok(outcome)
		}
	}

	fn contains_id(&self, id: &tg::Id) -> impl Future<Output = tg::Result<bool>> + Send {
		self.contains_ids(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn contains_ids(&self, ids: &[tg::Id]) -> impl Future<Output = tg::Result<Vec<bool>>> + Send;

	fn clean_usage(
		&self,
		arg: crate::usage::clean::Arg,
	) -> impl Future<Output = tg::Result<crate::usage::clean::Output>> + Send;

	fn aggregate_usage(
		&self,
		arg: crate::usage::aggregate::Arg,
	) -> impl Future<Output = tg::Result<crate::usage::aggregate::Output>> + Send;

	fn visible(
		&self,
		ids: &[tg::Id],
		principal: &tg::Principal,
	) -> impl Future<Output = tg::Result<Vec<bool>>> + Send;

	fn batch(&self, arg: crate::batch::Arg) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_ancestors(
		&self,
		id: &tg::Id,
	) -> impl Future<Output = tg::Result<Option<Vec<tg::Id>>>> + Send;

	fn try_get_checkouts(
		&self,
		ids: &[tg::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::checkout::Checkout>>>> + Send;

	fn try_get_checkout(
		&self,
		id: &tg::Id,
	) -> impl Future<Output = tg::Result<Option<crate::checkout::Checkout>>> + Send {
		self.try_get_checkouts(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn try_get_groups(
		&self,
		ids: &[tg::group::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::group::Group>>>> + Send;

	fn try_get_group(
		&self,
		id: &tg::group::Id,
	) -> impl Future<Output = tg::Result<Option<crate::group::Group>>> + Send {
		self.try_get_groups(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

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

	fn try_get_organizations(
		&self,
		ids: &[tg::organization::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::organization::Organization>>>> + Send;

	fn try_get_organization(
		&self,
		id: &tg::organization::Id,
	) -> impl Future<Output = tg::Result<Option<crate::organization::Organization>>> + Send {
		self.try_get_organizations(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn get_usage(
		&self,
		account: &crate::usage::Account,
		period: crate::usage::Period,
		now: jiff::Timestamp,
	) -> impl Future<Output = tg::Result<crate::usage::Aggregate>> + Send;

	fn start_usage(&self, at: jiff::Timestamp) -> impl Future<Output = tg::Result<()>> + Send;

	fn touch_checkouts(
		&self,
		ids: &[tg::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Vec<Option<crate::checkout::Checkout>>>> + Send;

	fn touch_checkout(
		&self,
		id: &tg::Id,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Option<crate::checkout::Checkout>>> + Send {
		self.touch_checkouts(std::slice::from_ref(id), touched_at, time_to_touch)
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn try_get_object_children(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<Vec<tg::object::Id>>>> + Send;

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

	fn touch_objects_with_account(
		&self,
		ids: &[tg::object::Id],
		account: Option<&crate::usage::Account>,
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

	fn touch_object_with_account(
		&self,
		id: &tg::object::Id,
		account: Option<&crate::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Option<crate::object::Object>>> + Send {
		self.touch_objects_with_account(
			std::slice::from_ref(id),
			account,
			touched_at,
			time_to_touch,
		)
		.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::process::Process>>>> + Send;

	fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> impl Future<Output = tg::Result<Option<Vec<tg::process::data::Child>>>> + Send;

	fn try_get_process_node_children(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<crate::process::NodeChildren>>> + Send;

	fn try_get_cached_processes(
		&self,
		command: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Vec<(tg::process::Id, crate::process::Process)>>> + Send;

	fn get_requester_subjects(
		&self,
		principal: &tg::Principal,
	) -> impl Future<Output = tg::Result<Vec<tg::authorization::Subject>>> + Send;

	fn list_sandboxes_for_creator(
		&self,
		creator: &tg::Principal,
	) -> impl Future<Output = tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>>> + Send;

	fn list_sandboxes_for_owner(
		&self,
		owner: &tg::Principal,
	) -> impl Future<Output = tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>>> + Send;

	fn get_runner_sandboxes(
		&self,
		runner: &tg::runner::Id,
	) -> impl Future<Output = tg::Result<Vec<tg::sandbox::Id>>> + Send;

	fn get_sandbox_processes(
		&self,
		sandbox: &tg::sandbox::Id,
	) -> impl Future<Output = tg::Result<Vec<(tg::process::Id, crate::process::Process)>>> + Send;

	fn list_sandboxes(
		&self,
	) -> impl Future<Output = tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>>> + Send;

	fn process_has_ancestor(
		&self,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> impl Future<Output = tg::Result<bool>> + Send;

	fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<crate::process::Process>>> + Send {
		self.try_get_processes(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn try_get_sandboxes(
		&self,
		ids: &[tg::sandbox::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::sandbox::Sandbox>>>> + Send;

	fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
	) -> impl Future<Output = tg::Result<Option<crate::sandbox::Sandbox>>> + Send {
		self.try_get_sandboxes(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

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

	fn try_get_tags(
		&self,
		ids: &[tg::tag::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::tag::Tag>>>> + Send;

	fn try_get_tag(
		&self,
		id: &tg::tag::Id,
	) -> impl Future<Output = tg::Result<Option<crate::tag::Tag>>> + Send {
		self.try_get_tags(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn try_get_users(
		&self,
		ids: &[tg::user::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::user::User>>>> + Send;

	fn try_get_user(
		&self,
		id: &tg::user::Id,
	) -> impl Future<Output = tg::Result<Option<crate::user::User>>> + Send {
		self.try_get_users(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Vec<Option<crate::process::Process>>>> + Send;

	fn touch_processes_and_put_account(
		&self,
		ids: &[tg::process::Id],
		account: &crate::usage::Account,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Vec<Option<crate::process::Process>>>> + Send;

	fn touch_processes_with_account(
		&self,
		ids: &[tg::process::Id],
		account: Option<&crate::usage::Account>,
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

	fn touch_process_and_put_account(
		&self,
		id: &tg::process::Id,
		account: &crate::usage::Account,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Option<crate::process::Process>>> + Send {
		self.touch_processes_and_put_account(
			std::slice::from_ref(id),
			account,
			touched_at,
			time_to_touch,
		)
		.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_process_with_account(
		&self,
		id: &tg::process::Id,
		account: Option<&crate::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Option<crate::process::Process>>> + Send {
		self.touch_processes_with_account(
			std::slice::from_ref(id),
			account,
			touched_at,
			time_to_touch,
		)
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

	fn complete_log_compaction(
		&self,
		entry: &crate::log::Entry,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn enqueue_log_compaction(
		&self,
		process: &tg::process::Id,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn log_compaction_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> impl Future<Output = tg::Result<Vec<crate::log::Entry>>> + Send;

	fn try_get_oldest_log_compaction_transaction_id(
		&self,
	) -> impl Future<Output = tg::Result<Option<u64>>> + Send;

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

	fn partition_total(&self) -> u64;

	fn usage_partition_total(&self) -> u64;
}
