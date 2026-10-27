use {
	crate::{Origin, Server, Session},
	futures::{future::BoxFuture, stream::BoxStream},
	std::{ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_http::body::Boxed as BoxBody,
};

pub type ConnectionFuture = BoxFuture<'static, tg::Result<()>>;

/// The sandbox component.
pub trait Component: crate::component::Lifecycle {
	fn create_sandbox<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::sandbox::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::sandbox::create::Output>>;

	fn try_get_sandbox<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::sandbox::get::Output>>>;

	fn try_get_sandbox_processes_stream<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>>,
	>;

	fn list_sandboxes<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::sandbox::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::sandbox::list::Output>>;

	fn try_destroy_sandbox<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> BoxFuture<'a, tg::Result<Option<bool>>>;

	fn try_get_sandbox_status_stream<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>>>;

	fn try_wait_sandbox_future<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::wait::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::sandbox::outcome::Data>>>>>,
	>;

	fn get_sandbox_control_stream<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				(
					tg::sandbox::control::Header,
					BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
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

	fn authorize_with_sandbox_control_service<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		principal: &'a tg::Principal,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn create_sandbox_status_stream_local_with_wakeups<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		initial: Option<tg::sandbox::Data>,
		wakeups: Option<BoxStream<'static, ()>>,
		source: tg::sandbox::Source,
	) -> BoxStream<'static, tg::Result<tg::sandbox::status::Event>>;

	fn create_sandbox_status_wakeup_stream<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		stopper: Option<Stopper>,
		timeout: Option<Duration>,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, ()>>>;

	fn destroy_sandbox_when_available<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
		connection_future: ConnectionFuture,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn get_sandbox_from_index<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<tangram_index::sandbox::Sandbox>>;

	fn prepare_sandbox_control_index_arg<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		created_at: i64,
		create: tg::sandbox::create::Arg,
		creator: Option<tg::Principal>,
		runner: Option<tg::runner::Id>,
	) -> BoxFuture<'a, tg::Result<tangram_index::sandbox::put::Arg>>;

	fn publish_sandbox_process_spawned<'a>(
		&'a self,
		server: &'a Server,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn request_sandbox_control<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::control::ServerRequestArg,
		options: crate::control::Options,
	) -> BoxFuture<'a, tg::Result<tg::Result<tg::sandbox::control::ClientResponseOutput>>>;

	fn spawn_publish_sandbox_status_task(&self, server: &Server, id: &tg::sandbox::Id);

	fn subscribe_sandbox_connection<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<ConnectionFuture>>;

	fn try_get_sandbox_from_index<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<Option<tangram_index::sandbox::Sandbox>>>;

	fn validate_sandbox_create_arg_with_parent<'a>(
		&'a self,
		session: &'a Session,
		arg: &'a tg::sandbox::create::Arg,
		parent: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<()>>;
}

impl crate::Server {
	pub fn origin_has_network_access(&self, origin: Origin) -> tg::Result<bool> {
		let sandbox = self.try_get_request_origin_sandbox(origin)?;
		let has_network_access = sandbox.is_none_or(|sandbox| sandbox.create.network.is_some());

		Ok(has_network_access)
	}

	pub fn try_get_request_origin_sandbox(
		&self,
		origin: Origin,
	) -> tg::Result<
		Option<dashmap::mapref::one::Ref<'_, u64, tangram_server_runner_core::sandbox::State>>,
	> {
		let Ok(index) = origin.try_unwrap_sandbox() else {
			return Ok(None);
		};
		let sandbox = self
			.components
			.runner
			.state()
			.sandboxes()
			.get(index)
			.ok_or_else(|| tg::error!(%index, "failed to find the origin sandbox"))?;

		Ok(Some(sandbox))
	}
}
