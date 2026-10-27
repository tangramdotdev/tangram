use {
	crate::{Server, Session},
	futures::{future::BoxFuture, stream::BoxStream},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_http::body::Boxed as BoxBody,
};

pub struct PushProcessObjectsArg {
	/// The sync that the push joins, when the push does not know the sync.
	pub join: Option<tg::sync::Join>,

	pub location: tg::Location,

	pub nodes: Vec<tg::Referent<tg::Id>>,

	pub process_command_objects: bool,

	pub sync: Option<tg::Referent<tg::sync::Id>>,
}

pub struct PushProcessObjectsOutput {
	pub sync: tg::Referent<tg::sync::Id>,
	pub task: Task<tg::Result<()>>,
}

pub struct TryGetProcessRegionsArg<'a> {
	pub availability: bool,
	pub id: &'a tg::process::Id,
	pub metadata: bool,
	pub regions: &'a [String],
	pub source: tg::process::Source,
	pub tokens: &'a tg::authorization::Tokens,
}

/// The process component.
pub trait Component: crate::component::Lifecycle {
	fn try_get_process_local<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		metadata: bool,
		availability: bool,
		tokens: &'a [tg::authorization::Token],
		source: tg::process::Source,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::get::Output>>>;

	fn try_get_process_connect_stream<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::process::connect::Arg,
		input: BoxStream<'static, tg::Result<tg::process::connect::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				Option<(
					tg::process::connect::Header,
					BoxStream<'static, tg::Result<tg::process::connect::ServerMessage>>,
				)>,
				tg::Error,
			>,
		>,
	>;

	fn try_spawn_process<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::process::spawn::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<
			BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>,
		>,
	>;

	fn try_get_process_metadata<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::Metadata>>>;

	fn try_get_process_availability<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		arg: tg::process::availability::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::Availability>>>;

	fn try_get_process<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		arg: tg::process::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::get::Output>>>;

	fn put_process<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		arg: tg::process::put::Arg,
	) -> BoxFuture<'a, tg::Result<tg::process::put::Output>>;

	fn try_cancel_process<'a>(
		&'a self,
		session: &'a Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::cancel::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::cancel::Output>>>;

	fn try_get_process_control_stream<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				Option<(
					tg::process::control::Header,
					BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
				)>,
				tg::Error,
			>,
		>,
	>;

	fn try_signal_process<'a>(
		&'a self,
		session: &'a Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::signal::post::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_get_process_status_stream<'a>(
		&'a self,
		session: &'a Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::status::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>>>;

	fn try_get_process_children_stream<'a>(
		&'a self,
		session: &'a Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::children::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>>,
	>;

	fn try_set_process_tty_size<'a>(
		&'a self,
		session: &'a Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::tty::size::put::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_read_process_stdio<'a>(
		&'a self,
		session: &'a Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>,
				tg::Error,
			>,
		>,
	>;

	fn try_write_process_stdio<'a>(
		&'a self,
		session: &'a Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Output>>>,
				tg::Error,
			>,
		>,
	>;

	fn try_touch_process<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_wait_process_future<'a>(
		&'a self,
		session: &'a Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::wait::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>>>,
	>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn authorize_with_process_control_service<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		principal: &'a tg::Principal,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn create_process_sandbox_permission_arg<'a>(
		&'a self,
		session: &'a Session,
		process: &'a tg::process::Id,
		sandbox: &'a tg::sandbox::Id,
		created_at: i64,
	) -> tg::Result<tangram_index::permission::put::Arg>;

	fn expire_process_log_cache<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		data: &'a tg::process::Data,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn get_process_local<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		metadata: bool,
	) -> BoxFuture<'a, tg::Result<tg::process::get::Output>>;

	fn notify_process_log(&self, server: &Server, id: &tg::process::Id);

	fn push_process_objects<'a>(
		&'a self,
		session: &'a Session,
		arg: PushProcessObjectsArg,
	) -> BoxFuture<'a, tg::Result<PushProcessObjectsOutput>>;

	fn put_process_local<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		arg: tg::process::put::Arg,
		options: tangram_server_process_core::put::Options,
	) -> BoxFuture<'a, tg::Result<tg::process::put::Output>>;

	fn put_process_local_inner<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		arg: tg::process::put::Arg,
		principal: tg::Principal,
		options: tangram_server_process_core::put::Options,
	) -> BoxFuture<'a, tg::Result<tg::process::put::Output>>;

	fn request_process_control<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		arg: tg::process::control::ServerRequestArg,
		options: crate::control::Options,
	) -> BoxFuture<'a, tg::Result<tg::Result<tg::process::control::ClientResponseOutput>>>;

	fn set_process_children_from_index<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		children_set: bool,
		data: &'a mut tg::process::Data,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn spawn_process_finish_tasks<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
	) -> ();

	fn spawn_publish_process_status_task(&self, server: &Server, id: &tg::process::Id);

	fn store_process_error<'a>(
		&'a self,
		session: &'a Session,
		error: tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>,
	) -> BoxFuture<'a, tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>>;

	fn try_acquire_scheduled_sandbox_capacity<'a>(
		&'a self,
		session: &'a Session,
		borrowed: bool,
		parent: Option<&'a tg::sandbox::Id>,
		requested: tg::runner::Capacity,
	) -> Option<tangram_server_runner_core::capacity::Allocation>;

	fn try_get_process_local_inner<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		metadata: bool,
		source: tg::process::Source,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::get::Output>>>;

	fn try_get_process_local_with_permissions<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		permissions: tg::authorization::permission::Set,
		metadata: bool,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::get::Output>>>;

	fn try_get_process_parent_from_control_service<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::Id>>>;

	fn try_get_process_regions<'a>(
		&'a self,
		session: &'a Session,
		arg: TryGetProcessRegionsArg<'a>,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::get::Output>>>;

	fn try_get_process_with_data_from_index<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
	) -> BoxFuture<
		'a,
		tg::Result<Option<(tangram_index::process::Process, Option<tg::process::Data>)>>,
	>;

	fn try_resolve_process_command_sync<'a>(
		&'a self,
		session: &'a Session,
		nonce: &'a tg::process::Nonce,
	) -> BoxFuture<'a, tg::Result<Option<tg::Referent<tg::sync::Id>>>>;

	fn try_resolve_process_control_sync<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		kind: tg::sync::join::Kind,
	) -> BoxFuture<'a, tg::Result<Option<tg::Referent<tg::sync::Id>>>>;
}
