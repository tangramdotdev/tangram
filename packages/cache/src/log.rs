use tangram_client::prelude::*;

pub mod cache;
pub mod delete;
pub mod end;
pub mod length;
pub mod put;
pub mod read;

pub trait Cache {
	fn delete_log_cache_entry(
		&self,
		arg: crate::log::cache::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_log_cache_entries(
		&self,
		arg: crate::log::cache::get::Arg,
	) -> impl Future<Output = tg::Result<Vec<crate::log::cache::Entry>>> + Send;

	fn put_log_cache_entry(
		&self,
		arg: crate::log::cache::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_log(
		&self,
		arg: crate::log::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_log(&self, arg: crate::log::put::Arg) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_log_batch(
		&self,
		args: Vec<crate::log::put::Arg>,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_log_end(&self, arg: crate::log::end::Arg)
	-> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_log_end(
		&self,
		process: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<tg::process::log::End>>> + Send;

	fn try_get_log_length(
		&self,
		arg: crate::log::length::Arg,
	) -> impl Future<Output = tg::Result<Option<u64>>> + Send;

	fn try_read_log(
		&self,
		arg: crate::log::read::Arg,
	) -> impl Future<Output = tg::Result<Vec<crate::log::read::Entry<'static>>>> + Send;
}
