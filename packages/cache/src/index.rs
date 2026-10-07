use tangram_client::prelude::*;

pub mod queue;

pub trait Cache {
	fn delete_index_queue_fragment(
		&self,
		arg: crate::index::queue::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_index_queue_fragments(
		&self,
		arg: crate::index::queue::get::batch::Arg,
	) -> impl Future<Output = tg::Result<Vec<crate::index::queue::Fragment>>> + Send;

	fn put_index_queue_fragment(
		&self,
		arg: crate::index::queue::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_index_queue_fragment(
		&self,
		arg: crate::index::queue::get::Arg,
	) -> impl Future<Output = tg::Result<Option<crate::index::queue::Fragment>>> + Send;
}
