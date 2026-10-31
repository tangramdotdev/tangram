use {
	dashmap::DashMap,
	futures::{FutureExt as _, future::BoxFuture, stream::BoxStream},
	std::{ops::ControlFlow, sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::{
		Server, Session, messenger::Messenger, runner::Runner, sandbox::ConnectionFuture,
	},
	tangram_server_service as service,
};

mod notification;
mod runner;

pub mod control;
pub mod create;
pub mod destroy;
pub mod get;
pub mod list;
pub mod processes;
pub mod start;
pub mod status;
pub mod stop;
pub mod wait;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub(crate) control: Control,
}

pub(crate) struct Control {
	pub client: service::Client<
		tg::sandbox::Id,
		self::control::service::Request,
		self::control::service::Response,
		Messenger,
	>,
	pub servers: DashMap<
		tg::sandbox::Id,
		service::Server<self::control::service::Request, self::control::service::Response>,
		fnv::FnvBuildHasher,
	>,
	pub service_tasks: tangram_futures::task::Set<tg::Result<()>>,
}

impl Component {
	pub fn new(arg: &tangram_server_core::component::Arg<'_>) -> tg::Result<Self> {
		// Create the control client, which observes its events as checkpoints.
		let observer = Arc::new(service::Checkpoints::new(
			arg.checkpoints.cloned(),
			self::control::service::NAME,
		));
		let client = service::Client::new(
			arg.messenger.clone(),
			self::control::service::SUBJECT,
			arg.config.sandbox.control_service,
			observer,
		)
		.map_err(|source| tg::error!(!source, "failed to create the sandbox control client"))?;
		let control = Control {
			client,
			servers: DashMap::default(),
			service_tasks: tangram_futures::task::Set::default(),
		};

		// Create the state.
		let state = State { control };
		let component = Self(Arc::new(state));

		Ok(component)
	}

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["sandboxes"]) => {
				self.create_sandbox_request(session, request).await
			},
			(http::Method::GET, ["sandboxes"]) => {
				self.list_sandboxes_request(session, request).await
			},
			(http::Method::GET, ["sandboxes", sandbox]) => {
				self.try_get_sandbox_request(session, request, sandbox)
					.await
			},
			(http::Method::POST, ["sandboxes", sandbox, "destroy"]) => {
				self.try_destroy_sandbox_request(session, request, sandbox)
					.await
			},
			(http::Method::POST, ["sandboxes", sandbox, "start"]) => {
				self.try_start_sandbox_request(session, request, sandbox)
					.await
			},
			(http::Method::POST, ["sandboxes", sandbox, "stop"]) => {
				self.try_stop_sandbox_request(session, request, sandbox)
					.await
			},
			(http::Method::POST, ["sandboxes", "control"]) => {
				self.get_sandbox_control_stream_request(session, request)
					.await
			},
			(http::Method::GET, ["sandboxes", sandbox, "processes"]) => {
				self.try_get_sandbox_processes_stream_request(session, request, sandbox)
					.await
			},
			(http::Method::GET, ["sandboxes", sandbox, "status"]) => {
				self.try_get_sandbox_status_stream_request(session, request, sandbox)
					.await
			},
			(http::Method::POST, ["sandboxes", sandbox, "wait"]) => {
				self.try_wait_sandbox_future_request(session, request, sandbox)
					.await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {
	fn start<'a>(&'a self, server: &'a Server) -> BoxFuture<'a, tg::Result<()>> {
		async move {
			// Reconcile the sandboxes that were running before the restart if multi-server mode is disabled.
			if !server.config.advanced.multi_server {
				match self.list_unfinished_sandboxes(server).await {
					Ok(sandboxes) => {
						let component = self.clone();
						let server = server.clone();
						tokio::spawn(async move {
							let result = component
								.reconcile_unfinished_sandboxes(&server, sandboxes)
								.await;
							if let Err(error) = result {
								tracing::error!(error = %error.trace(), "failed to reconcile unfinished sandboxes");
							}
						});
					},
					Err(error) => {
						tracing::error!(error = %error.trace(), "failed to list unfinished sandboxes");
					},
				}
			}
			Ok(())
		}
		.boxed()
	}

	fn stop<'a>(&'a self, _server: &'a Server) -> BoxFuture<'a, ()> {
		async move {
			// Stop the control client and drain the control service tasks.
			self.control.client.stop();
			self.control.service_tasks.stop_all();
			self.control.service_tasks.wait().await;
		}
		.boxed()
	}

	fn abort(&self) {
		self.control.service_tasks.abort_all();
	}
}

impl tangram_server_core::sandbox::Component for Component {
	fn authorize_with_sandbox_control_service<'a>(
		&'a self,
		_session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		principal: &'a tg::Principal,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.authorize_with_sandbox_control_service(id, principal)
			.boxed()
	}

	fn create_sandbox_status_stream_local_with_wakeups<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		initial: Option<tg::sandbox::Data>,
		wakeups: Option<BoxStream<'static, ()>>,
		source: tg::sandbox::Source,
	) -> BoxStream<'static, tg::Result<tg::sandbox::status::Event>> {
		self.create_sandbox_status_stream_local_with_wakeups(session, id, initial, wakeups, source)
	}

	fn create_sandbox_status_wakeup_stream<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		stopper: Option<Stopper>,
		timeout: Option<Duration>,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, ()>>> {
		self.create_sandbox_status_wakeup_stream(session, id, stopper, timeout)
			.boxed()
	}

	fn destroy_sandbox_when_available<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
		connection_future: ConnectionFuture,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.destroy_sandbox_when_available(session, id, arg, connection_future)
			.boxed()
	}

	fn get_sandbox_from_index<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<tangram_index::sandbox::Sandbox>> {
		self.get_sandbox_from_index(session, id).boxed()
	}

	fn prepare_sandbox_control_index_arg<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		create: tg::process::control::SandboxCreate,
	) -> BoxFuture<'a, tg::Result<tangram_index::sandbox::put::Arg>> {
		self.prepare_sandbox_control_index_arg(session, id, create)
			.boxed()
	}

	fn publish_sandbox_process_spawned<'a>(
		&'a self,
		server: &'a Server,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<()>> {
		Self::publish_sandbox_process_spawned(server, id).boxed()
	}

	fn request_sandbox_control<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::control::ServerRequestArg,
		options: tangram_server_core::control::Options,
	) -> BoxFuture<'a, tg::Result<tg::Result<tg::sandbox::control::ClientResponseOutput>>> {
		self.request_sandbox_control(session, id, arg, options)
			.boxed()
	}

	fn spawn_publish_sandbox_status_task(&self, server: &Server, id: &tg::sandbox::Id) {
		Self::spawn_publish_sandbox_status_task(server, id);
	}

	fn subscribe_sandbox_connection<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<ConnectionFuture>> {
		self.subscribe_sandbox_connection(session, id).boxed()
	}

	fn try_get_sandbox_from_index<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<Option<tangram_index::sandbox::Sandbox>>> {
		self.try_get_sandbox_from_index(session, id).boxed()
	}

	fn validate_sandbox_create_arg_with_parent<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: &'a tg::sandbox::create::Arg,
		parent: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.validate_sandbox_create_arg_with_parent(session, arg, parent)
			.boxed()
	}

	fn create_sandbox<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::sandbox::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::sandbox::create::Output>> {
		self.create_sandbox(session, arg).boxed()
	}

	fn try_get_sandbox<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::sandbox::get::Output>>> {
		self.try_get_sandbox(session, id, arg).boxed()
	}

	fn try_get_sandbox_processes_stream<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>>,
	> {
		self.try_get_sandbox_processes_stream(session, id, arg)
			.boxed()
	}

	fn list_sandboxes<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::sandbox::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::sandbox::list::Output>> {
		self.list_sandboxes(session, arg).boxed()
	}

	fn try_destroy_sandbox<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> BoxFuture<'a, tg::Result<Option<bool>>> {
		self.try_destroy_sandbox(session, id, arg).boxed()
	}

	fn try_stop_sandbox<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::stop::Arg,
	) -> BoxFuture<'a, tg::Result<Option<bool>>> {
		self.try_stop_sandbox(session, id, arg).boxed()
	}

	fn try_start_sandbox<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::start::Arg,
	) -> BoxFuture<'a, tg::Result<Option<bool>>> {
		self.try_start_sandbox(session, id, arg).boxed()
	}

	fn try_get_sandbox_status_stream<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>>>
	{
		self.try_get_sandbox_status_stream(session, id, arg).boxed()
	}

	fn try_wait_sandbox_future<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::wait::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::sandbox::outcome::Data>>>>>,
	> {
		self.try_wait_sandbox_future(session, id, arg).boxed()
	}

	fn get_sandbox_control_stream<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
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
	> {
		self.get_sandbox_control_stream_with_context(session, arg, stream)
			.boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
