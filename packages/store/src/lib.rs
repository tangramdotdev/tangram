#[cfg(feature = "lmdb")]
mod read;

pub mod capacity;
#[cfg(feature = "lmdb")]
pub mod lmdb;
pub mod log;
pub mod memory;
pub mod object;
pub mod prelude;
#[cfg(feature = "scylla")]
pub mod scylla;

pub trait Store {
	fn delete_object_cache_entry(
		&self,
		arg: object::cache::delete::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn delete_object_archive_outbox_entries(
		&self,
		arg: object::archive::outbox::delete::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn delete_log(
		&self,
		arg: log::delete::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn delete_object(
		&self,
		arg: object::delete::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn delete_object_batch(
		&self,
		args: Vec<object::delete::Arg>,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn delete_object_index_outbox_fragments(
		&self,
		arg: object::index::outbox::fragment::delete::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn dequeue_object_index_outbox_fragments(
		&self,
		arg: object::index::outbox::fragment::dequeue::Arg,
	) -> impl std::future::Future<
		Output = tangram_client::Result<Vec<object::index::outbox::fragment::Fragment>>,
	> + Send;

	fn dequeue_object_archive_outbox_entries(
		&self,
		arg: object::archive::outbox::dequeue::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Vec<object::archive::outbox::Entry>>>
	+ Send;

	fn get_object_cache_entries(
		&self,
		arg: object::cache::get::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Vec<object::cache::Entry>>> + Send;

	fn put_object_cache_entry(
		&self,
		arg: object::cache::put::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_object_archive_outbox_entries(
		&self,
		arg: object::archive::outbox::put::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn enqueue_object_index_outbox_batch(
		&self,
		arg: object::index::outbox::batch::enqueue::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn flush(&self) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_log(
		&self,
		arg: log::put::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_log_batch(
		&self,
		args: Vec<log::put::Arg>,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_object(
		&self,
		arg: object::put::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_object_batch(
		&self,
		args: Vec<object::put::Arg>,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn try_get_log_length(
		&self,
		arg: log::length::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Option<u64>>> + Send;

	fn try_get_object(
		&self,
		arg: object::get::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<object::get::Output>> + Send;

	fn try_get_object_batch(
		&self,
		arg: object::get::batch::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Vec<object::get::Output>>> + Send;

	fn try_get_object_index_outbox_batch_at_or_before(
		&self,
		arg: object::index::outbox::batch::get::Arg,
	) -> impl std::future::Future<
		Output = tangram_client::Result<Option<object::index::outbox::batch::Id>>,
	> + Send;

	fn try_get_capacity(
		&self,
	) -> impl std::future::Future<Output = tangram_client::Result<Option<capacity::Capacity>>> + Send;

	fn try_read_log(
		&self,
		arg: log::read::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Vec<log::read::Entry<'static>>>> + Send;
}
