use tangram_client::prelude::*;

pub mod queue;

pub trait Cache {
	fn delete_index_queue_item(
		&self,
		arg: crate::index::queue::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_index_queue_item_batch(
		&self,
		arg: crate::index::queue::delete::batch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_index_queue_items(
		&self,
		arg: crate::index::queue::get::batch::Arg,
	) -> impl Future<Output = tg::Result<Vec<crate::index::queue::Item>>> + Send;

	fn put_index_queue_item(
		&self,
		arg: crate::index::queue::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_index_queue_item(
		&self,
		arg: crate::index::queue::get::Arg,
	) -> impl Future<Output = tg::Result<Option<crate::index::queue::Item>>> + Send;
}
