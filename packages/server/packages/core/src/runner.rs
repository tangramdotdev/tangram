use {
	crate::{Server, Session},
	futures::{future::BoxFuture, stream::BoxStream},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The runner component.
pub trait Component: crate::component::Lifecycle {
	fn try_get_runner_data<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
	) -> BoxFuture<'a, tg::Result<Option<tg::runner::Data>>>;

	fn create_runner<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::runner::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::create::Output>>;

	fn create_runner_token<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
		arg: tg::runner::token::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::token::create::Output>>;

	fn try_delete_runner<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_delete_runner_token<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
		token: &'a tg::token::Id,
		arg: tg::runner::token::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_runners<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::runner::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::list::Output>>;

	fn list_runner_tokens<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
		arg: tg::runner::token::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::token::list::Output>>;

	fn get_runner_control_stream<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				(
					tg::runner::control::Header,
					BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>,
				),
				tg::Error,
			>,
		>,
	>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn resolve_sandbox_backend(&self, server: &Server) -> tg::Result<tangram_sandbox::Backend>;

	fn send_runner_control_request<'a>(
		&'a self,
		session: &'a Session,
		runner: &'a tg::runner::Id,
		scheduler: &'a tg::scheduler::Id,
		connection_index: u64,
		arg: tg::runner::control::ServerRequestArg,
		options: crate::control::Options,
	) -> BoxFuture<'a, tg::Result<tg::Result<tg::runner::control::ClientResponseOutput>>>;

	fn spawn_sandbox_task<'a>(
		&'a self,
		server: &'a Server,
		arg: tangram_server_runner_core::sandbox::SpawnSandboxTaskArg,
	) -> tangram_server_runner_core::sandbox::SpawnSandboxTaskOutput;

	fn start_sandbox_pool<'a>(&'a self, session: &'a Session) -> ();

	fn state(&self) -> &tangram_server_runner_core::State;

	fn stop_runner_task(&self);

	fn stop_sandbox_pool(&self) -> BoxFuture<'_, ()>;

	fn try_get_process_inner_token<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
	) -> Option<String>;
}
