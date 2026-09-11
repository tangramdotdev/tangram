#[cfg(feature = "lmdb")]
mod read;

pub mod archive;
pub mod capacity;
pub mod index;
#[cfg(feature = "lmdb")]
pub mod lmdb;
pub mod log;
pub mod memory;
pub mod object;
pub mod prelude;
#[cfg(feature = "scylla")]
pub mod scylla;

pub trait Cache {
	fn contains_object(
		&self,
		arg: object::contains::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<bool>> + Send;

	fn delete_object_cache_entry(
		&self,
		arg: object::cache::delete::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn delete_archive_queue_entry(
		&self,
		arg: archive::queue::delete::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn delete_index_queue_fragment(
		&self,
		arg: index::queue::delete::Arg,
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

	fn get_archive_queue_entries(
		&self,
		arg: archive::queue::get::batch::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Vec<archive::queue::Entry>>> + Send;

	fn get_index_queue_fragments(
		&self,
		arg: index::queue::get::batch::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Vec<index::queue::Fragment>>> + Send;

	fn put_object_cache_entry(
		&self,
		arg: object::cache::put::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_archive_queue_entry(
		&self,
		arg: archive::queue::put::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_index_queue_fragment(
		&self,
		arg: index::queue::put::Arg,
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

	fn put_log_end(
		&self,
		arg: log::end::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn try_get_log_end(
		&self,
		process: &tangram_client::process::Id,
	) -> impl std::future::Future<
		Output = tangram_client::Result<Option<tangram_client::process::log::End>>,
	> + Send;

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

	fn try_get_archive_queue_entry(
		&self,
		arg: archive::queue::get::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Option<archive::queue::Entry>>> + Send;

	fn try_get_index_queue_fragment(
		&self,
		arg: index::queue::get::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<Option<index::queue::Fragment>>> + Send;

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
}
