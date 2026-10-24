use {
	crate::{Server, Session},
	futures::future::BoxFuture,
	tangram_client::prelude::*,
};

/// The scheduler component.
pub trait Component: crate::component::Lifecycle {
	fn destroy_runner_sandbox<'a>(
		&'a self,
		server: &'a Server,
		id: &'a tg::sandbox::Id,
		reason: &'a str,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn dequeue_sandbox<'a>(
		&'a self,
		session: &'a Session,
		sandbox: &'a tg::sandbox::Id,
		scheduler: &'a tg::scheduler::Id,
	) -> BoxFuture<'a, tg::Result<bool>>;

	fn enqueue_sandbox<'a>(
		&'a self,
		session: &'a Session,
		request: tangram_server_scheduler_core::EnqueueSandboxRequestArg,
	) -> BoxFuture<'a, tg::Result<tg::scheduler::Id>>;

	fn scheduler_heartbeat_expired<'a>(
		&'a self,
		session: &'a Session,
		scheduler: &'a tg::scheduler::Id,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn send_scheduler_request<'a>(
		&'a self,
		session: &'a Session,
		arg: tangram_server_scheduler_core::RequestArg,
	) -> BoxFuture<
		'a,
		tg::Result<(
			tg::scheduler::Id,
			tg::Result<tangram_server_scheduler_core::ResponseOutput>,
		)>,
	>;

	fn send_scheduler_request_to<'a>(
		&'a self,
		session: &'a Session,
		scheduler: &'a tg::scheduler::Id,
		arg: tangram_server_scheduler_core::RequestArg,
	) -> BoxFuture<'a, tg::Result<tg::Result<tangram_server_scheduler_core::ResponseOutput>>>;

	fn started(&self) -> BoxFuture<'_, ()>;
}
