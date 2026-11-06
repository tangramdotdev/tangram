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
		error: tg::error::Data,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn destroy_stopped_runner_sandbox<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
		sandbox: &'a tg::sandbox::Id,
		error: Option<tg::error::Data>,
	) -> BoxFuture<'a, tg::Result<bool>>;

	fn dequeue_sandbox<'a>(
		&'a self,
		session: &'a Session,
		sandbox: &'a tg::sandbox::Id,
		scheduler: &'a tg::scheduler::Id,
	) -> BoxFuture<'a, tg::Result<bool>>;

	fn acquire_process_lease<'a>(
		&'a self,
		session: &'a Session,
		scheduler: &'a tg::scheduler::Id,
		sandbox: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::Lease>>>;

	fn release_process_lease<'a>(
		&'a self,
		session: &'a Session,
		scheduler: &'a tg::scheduler::Id,
		sandbox: &'a tg::sandbox::Id,
		lease: tg::process::Lease,
	) -> BoxFuture<'a, tg::Result<Option<bool>>>;

	fn cancel_sandbox_stop<'a>(
		&'a self,
		session: &'a Session,
		scheduler: &'a tg::scheduler::Id,
		sandbox: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<bool>>;

	fn end_sandbox<'a>(
		&'a self,
		session: &'a Session,
		scheduler: &'a tg::scheduler::Id,
		arg: tangram_server_scheduler_core::EndSandboxRequestArg,
	) -> BoxFuture<'a, tg::Result<bool>>;

	fn enqueue_sandbox<'a>(
		&'a self,
		session: &'a Session,
		request: tangram_server_scheduler_core::EnqueueSandboxRequestArg,
	) -> BoxFuture<'a, tg::Result<tg::scheduler::Id>>;

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

	fn stop_runner_sandbox<'a>(
		&'a self,
		server: &'a Server,
		id: &'a tg::sandbox::Id,
		error: tg::error::Data,
	) -> BoxFuture<'a, tg::Result<()>>;
}
