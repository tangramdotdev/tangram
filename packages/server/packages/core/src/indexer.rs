use {
	crate::{Server, Session, database::Transaction},
	futures::{future::BoxFuture, stream::BoxStream},
	std::{ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The indexer component.
pub trait Component: crate::component::Lifecycle {
	fn index<'a>(
		&'a self,
		session: &'a Session,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<()>>>>>;

	fn index_batch<'a>(
		&'a self,
		server: &'a Server,
		arg: tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn index_batch_inner<'a>(
		&'a self,
		server: &'a Server,
		arg: tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<tg::Result<()>>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn clean_batch<'a>(
		&'a self,
		server: &'a Server,
		arg: CleanBatchArg,
	) -> BoxFuture<'a, tg::Result<tangram_index::clean::Output>>;

	fn enqueue_database_index_queue_with_transaction<'a>(
		&'a self,
		server: &'a Server,
		transaction: &'a Transaction<'_>,
		arg: &'a tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(), crate::database::Error>>>;

	fn enqueue_object_archive<'a>(
		&'a self,
		server: &'a Server,
		object: tg::object::Id,
		put: [u8; 16],
	) -> BoxFuture<'a, tg::Result<()>>;

	fn spawn_publish_database_index_queue_notification_task(&self, server: &Server);
}

pub struct CleanBatchArg {
	pub batch_size: usize,
	pub now: i64,
	pub object_time_to_live: Duration,
	pub partition_end: u64,
	pub partition_start: u64,
	pub process_time_to_live: Duration,
	pub sandbox_time_to_live: Duration,
}
