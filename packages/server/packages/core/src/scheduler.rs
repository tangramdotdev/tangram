use {
	crate::{Server, Session},
	futures::future::BoxFuture,
	tangram_client::prelude::*,
};

/// The scheduler component.
pub trait Component: crate::component::Lifecycle {
	fn add_sandbox_parent<'a>(
		&'a self,
		session: &'a Session,
		sandbox: &'a tg::sandbox::Id,
		parent: &'a tg::sandbox::Id,
		scheduler: &'a tg::scheduler::Id,
	) -> BoxFuture<'a, tg::Result<Option<tg::runner::Capacity>>>;

	/// End a sandbox of a runner as destroyed, finishing the processes of its boot if the caller takes the boot over, rather than its runner reporting it.
	fn destroy_runner_sandbox<'a>(
		&'a self,
		server: &'a Server,
		id: &'a tg::sandbox::Id,
		error: Option<tg::error::Data>,
		processes: bool,
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

	/// Get a sandbox owner and its ancestors, which an enqueue request carries.
	fn get_owner_ancestors<'a>(
		&'a self,
		session: &'a Session,
		owner: &'a tg::Principal,
	) -> BoxFuture<'a, tg::Result<Vec<tg::Id>>>;

	/// Select an available scheduler, such as the one that a start of a sandbox without a runner reserves its boot for.
	fn select_scheduler<'a>(
		&'a self,
		session: &'a Session,
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

	/// End a sandbox of a runner as stopped, finishing the processes of its boot if the caller takes the boot over, rather than its runner reporting it.
	fn stop_runner_sandbox<'a>(
		&'a self,
		server: &'a Server,
		id: &'a tg::sandbox::Id,
		error: tg::error::Data,
		processes: bool,
	) -> BoxFuture<'a, tg::Result<()>>;

	/// Enqueue a sandbox with a scheduler that accepts its host, returning `None` if every scheduler declines it.
	fn try_enqueue_sandbox<'a>(
		&'a self,
		session: &'a Session,
		request: tangram_server_scheduler_core::EnqueueSandboxRequestArg,
	) -> BoxFuture<'a, tg::Result<Option<tg::scheduler::Id>>>;
}
