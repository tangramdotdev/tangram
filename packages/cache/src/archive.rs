use tangram_client::prelude::*;

pub mod queue;

pub trait Cache {
	fn delete_archive_queue_item(
		&self,
		arg: crate::archive::queue::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_archive_queue_item_batch(
		&self,
		arg: crate::archive::queue::delete::batch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_archive_queue_items(
		&self,
		arg: crate::archive::queue::get::batch::Arg,
	) -> impl Future<Output = tg::Result<Vec<crate::archive::queue::Item>>> + Send;

	fn put_archive_queue_item(
		&self,
		arg: crate::archive::queue::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_archive_queue_item(
		&self,
		arg: crate::archive::queue::get::Arg,
	) -> impl Future<Output = tg::Result<Option<crate::archive::queue::Item>>> + Send;
}
