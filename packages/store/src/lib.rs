#[cfg(feature = "lmdb")]
mod read;

pub mod capacity;
pub mod indexer;
#[cfg(feature = "lmdb")]
pub mod lmdb;
pub mod log;
pub mod memory;
pub mod object;
pub mod prelude;
#[cfg(feature = "scylla")]
pub mod scylla;

pub trait Store {
	fn contains_object(
		&self,
		arg: object::contains::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<bool>> + Send;

	fn delete_object_cache_entry(
		&self,
		arg: object::cache::delete::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn delete_indexer(
		&self,
		arg: indexer::delete::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn delete_object_archive_queue_entry(
		&self,
		arg: object::archive::queue::delete::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn delete_object_index_queue_fragment(
		&self,
		arg: object::index::queue::delete::Arg,
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

	fn get_object_cache_entries(
		&self,
		arg: object::cache::get::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Vec<object::cache::Entry>>> + Send;

	fn get_indexers(
		&self,
	) -> impl std::future::Future<Output = tangram_client::Result<Vec<indexer::Indexer>>> + Send;

	fn put_object_cache_entry(
		&self,
		arg: object::cache::put::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_indexer(
		&self,
		arg: indexer::put::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_object_archive_queue_entry(
		&self,
		arg: object::archive::queue::put::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_object_index_queue_fragment(
		&self,
		arg: object::index::queue::put::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
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

	fn try_get_indexer(
		&self,
		arg: indexer::get::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Option<indexer::Indexer>>> + Send;

	fn try_get_object_archive_queue_entry(
		&self,
		arg: object::archive::queue::get::Arg,
	) -> impl std::future::Future<
		Output = tangram_client::Result<Option<object::archive::queue::Entry>>,
	> + Send;

	fn try_get_object_index_queue_fragment(
		&self,
		arg: object::index::queue::get::Arg,
	) -> impl std::future::Future<
		Output = tangram_client::Result<Option<object::index::queue::Fragment>>,
	> + Send;

	fn try_get_object(
		&self,
		arg: object::get::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<object::get::Output>> + Send;

	fn try_get_object_batch(
		&self,
		arg: object::get::batch::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Vec<object::get::Output>>> + Send;

	fn try_get_capacity(
		&self,
	) -> impl std::future::Future<Output = tangram_client::Result<Option<capacity::Capacity>>> + Send;

	fn try_read_log(
		&self,
		arg: log::read::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Vec<log::read::Entry<'static>>>> + Send;

	fn update_indexer(
		&self,
		arg: indexer::update::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;
}
