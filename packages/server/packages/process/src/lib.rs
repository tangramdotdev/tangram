use {
	dashmap::DashMap,
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture, stream::BoxStream},
	std::{ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::{
		Server, Session,
		messenger::Messenger,
		process::{PushProcessObjectsArg, PushProcessObjectsOutput},
	},
	tangram_server_process_core::{Child, Processes, put::Options},
	tangram_server_service as service,
};

mod notification;
mod push;
mod runner;
mod sandbox;

pub(crate) use runner::Runner;

pub mod attach;
pub mod availability;
pub mod cancel;
pub mod children;
pub mod control;
pub mod get;
pub mod metadata;
pub mod put;
pub mod signal;
pub mod spawn;
pub mod status;
pub mod stdio;
pub mod storage;
pub mod touch;
pub mod tty;
pub mod wait;

pub(crate) type ConnectionFuture =
	BoxFuture<'static, tg::Result<tangram_server_process_common::control::Connected>>;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub(crate) attach: Attach,
	pub(crate) control: Control,
	pub(crate) remote_put_tasks: tangram_futures::task::Set<()>,
}

pub(crate) struct Attach {
	pub client: service::Client<tg::process::Nonce, self::attach::service::Protocol, Messenger>,
	pub servers: DashMap<
		tg::process::Nonce,
		service::Server<self::attach::service::Protocol>,
		fnv::FnvBuildHasher,
	>,
	pub service_tasks: tangram_futures::task::Set<tg::Result<()>>,
}

pub(crate) struct Control {
	pub client: service::Client<tg::process::Id, self::control::service::Protocol, Messenger>,
	pub servers: DashMap<
		tg::process::Id,
		service::Server<self::control::service::Protocol>,
		fnv::FnvBuildHasher,
	>,
	pub service_tasks: tangram_futures::task::Set<tg::Result<()>>,
}

impl Component {
	pub fn new(arg: &tangram_server_core::component::Arg<'_>) -> tg::Result<Self> {
		// Create the attach client, which observes its events as checkpoints.
		let observer = Arc::new(service::Checkpoints::new(
			arg.checkpoints.cloned(),
			self::attach::service::NAME,
		));
		let client = service::Client::new(
			arg.messenger.clone(),
			self::attach::service::SUBJECT,
			arg.config.process.attach.service,
			observer,
		)
		.map_err(|source| tg::error!(!source, "failed to create the process attach client"))?;
		let attach = Attach {
			client,
			servers: DashMap::default(),
			service_tasks: tangram_futures::task::Set::default(),
		};

		// Create the control client, which observes its events as checkpoints.
		let observer = Arc::new(service::Checkpoints::new(
			arg.checkpoints.cloned(),
			self::control::service::NAME,
		));
		let client = service::Client::new(
			arg.messenger.clone(),
			self::control::service::SUBJECT,
			arg.config.process.control_service,
			observer,
		)
		.map_err(|source| tg::error!(!source, "failed to create the process control client"))?;
		let control = Control {
			client,
			servers: DashMap::default(),
			service_tasks: tangram_futures::task::Set::default(),
		};

		// Create the state.
		let state = State {
			attach,
			control,
			remote_put_tasks: tangram_futures::task::Set::default(),
		};
		let component = Self(Arc::new(state));

		Ok(component)
	}

	pub(crate) async fn mask_process_child_tokens(
		&self,
		session: &Session,
		id: &tg::process::Id,
		tokens: &tg::authorization::Tokens,
		children: &mut [tg::process::data::Child],
	) -> tg::Result<()> {
		if children
			.iter()
			.all(|child| child.process.options.tokens.is_empty())
		{
			return Ok(());
		}
		// Reading a process node does not grant its children's parent permissions.
		let resource = tg::Referent::with_node_and_local_tokens(
			id.clone(),
			tokens.local_authorization().to_vec(),
		);
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent,
		);
		let authorization = session
			.authorize(resource, permission)
			.await?
			.check_exhaustion()?;
		Self::mask_process_child_tokens_with_permissions(children, authorization.permissions);
		Ok(())
	}

	pub(crate) fn mask_process_child_tokens_with_permissions(
		children: &mut [tg::process::data::Child],
		permissions: tg::authorization::permission::Set,
	) {
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent,
		);
		if !permissions.contains(permission) {
			for child in children {
				child.process.options.tokens.clear();
			}
		}
	}

	pub(crate) fn process_permission_for_data(
		&self,
		session: &Session,
		data: &tg::process::Data,
	) -> tg::authorization::permission::process::Set {
		let mut permissions = tg::authorization::permission::process::Set::NODE;
		if Self::process_children_grant_subtree(
			session,
			data.children.as_deref().unwrap_or_default(),
		) {
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE);
		}
		let output = data.output_with_location_and_tokens();
		if self
			.process_output_has_subtree_permission(session, output.as_ref())
			.unwrap_or(true)
		{
			permissions.insert(tg::authorization::permission::process::Set::NODE_OUTPUT_OBJECTS);
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE_OUTPUT_OBJECTS);
		}
		let error = data.error_with_location_and_tokens();
		if Self::process_error_has_subtree_permission(session, error.as_ref()) {
			permissions.insert(tg::authorization::permission::process::Set::NODE_ERROR_OBJECTS);
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE_ERROR_OBJECTS);
		}
		if Self::process_log_has_subtree_permission(session, data.log.as_ref()) {
			permissions.insert(tg::authorization::permission::process::Set::NODE_LOG_OBJECTS);
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE_LOG_OBJECTS);
		}
		permissions
	}

	fn process_children_grant_subtree(
		session: &Session,
		children: &[tg::process::data::Child],
	) -> bool {
		children
			.iter()
			.all(|child| Self::process_token_grants_subtree(session, &child.process))
	}

	fn process_token_grants_subtree(
		session: &Session,
		process: &tg::Referent<tg::process::Id>,
	) -> bool {
		let tokens = process.options.tokens.local_authorization();
		let resource = tg::Selector::Id(process.node.clone().into());
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Subtree,
		);
		tokens.iter().any(|token| {
			session.server.components.authorization.authorize_token(
				session,
				&resource,
				permission.into(),
				token,
			)
		})
	}

	fn process_output_has_subtree_permission(
		&self,
		session: &Session,
		output: Option<&tg::value::Data>,
	) -> Option<bool> {
		output.map(|output| self.value_data_tokens_grant_subtree(session, output))
	}

	fn process_error_has_subtree_permission(
		session: &Session,
		error: Option<&tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>>,
	) -> bool {
		let Some(error) = error else {
			return true;
		};
		match error {
			tg::Either::Left(data) => {
				let mut children = std::collections::BTreeSet::new();
				data.children(&mut children);
				children.is_empty()
			},
			tg::Either::Right(error) => {
				Self::object_token_grants_subtree_for_process(session, error)
			},
		}
	}

	fn process_log_has_subtree_permission(
		session: &Session,
		log: Option<&tg::Referent<tg::blob::Id>>,
	) -> bool {
		log.is_none_or(|log| Self::object_token_grants_subtree_for_process(session, log))
	}

	fn value_data_tokens_grant_subtree(&self, session: &Session, data: &tg::value::Data) -> bool {
		match data {
			tg::value::Data::Array(array) => array
				.iter()
				.all(|value| self.value_data_tokens_grant_subtree(session, value)),
			tg::value::Data::Map(map) => map
				.values()
				.all(|value| self.value_data_tokens_grant_subtree(session, value)),
			tg::value::Data::Mutation(mutation) => {
				self.mutation_data_tokens_grant_subtree(session, mutation)
			},
			tg::value::Data::Module(module) => {
				let mut children = std::collections::BTreeSet::new();
				module.children(&mut children);
				children.into_iter().all(|id| {
					let object = tg::Referent::with_node_and_tokens(
						id,
						module.referent.options.tokens.clone(),
					);
					Self::object_token_grants_subtree_for_process(session, &object)
				})
			},
			tg::value::Data::Object(object) => {
				Self::object_token_grants_subtree_for_process(session, object)
			},
			tg::value::Data::Template(template) => {
				Self::template_data_tokens_grant_subtree(session, template)
			},
			tg::value::Data::Bool(_)
			| tg::value::Data::Bytes(_)
			| tg::value::Data::Null
			| tg::value::Data::Number(_)
			| tg::value::Data::Placeholder(_)
			| tg::value::Data::String(_) => true,
		}
	}

	fn mutation_data_tokens_grant_subtree(
		&self,
		session: &Session,
		data: &tg::mutation::Data,
	) -> bool {
		match data {
			tg::mutation::Data::Append { values } | tg::mutation::Data::Prepend { values } => {
				values
					.iter()
					.all(|value| self.value_data_tokens_grant_subtree(session, value))
			},
			tg::mutation::Data::Merge { value } => value
				.values()
				.all(|value| self.value_data_tokens_grant_subtree(session, value)),
			tg::mutation::Data::Prefix { template, .. }
			| tg::mutation::Data::Suffix { template, .. } => {
				Self::template_data_tokens_grant_subtree(session, template)
			},
			tg::mutation::Data::Set { value } | tg::mutation::Data::SetIfUnset { value } => {
				self.value_data_tokens_grant_subtree(session, value)
			},
			tg::mutation::Data::Unset => true,
		}
	}

	fn template_data_tokens_grant_subtree(session: &Session, data: &tg::template::Data) -> bool {
		data.components.iter().all(|component| match component {
			tg::template::data::Component::Artifact(artifact) => {
				Self::object_token_grants_subtree_for_process(session, artifact)
			},
			tg::template::data::Component::Placeholder(_)
			| tg::template::data::Component::String(_) => true,
		})
	}

	fn object_token_grants_subtree_for_process<T>(
		session: &Session,
		object: &tg::Referent<T>,
	) -> bool
	where
		T: Clone + Into<tg::Id>,
	{
		let tokens = object.options.tokens.local_authorization();
		let resource = tg::Selector::Id(object.node.clone().into());
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		tokens.iter().any(|token| {
			session.server.components.authorization.authorize_token(
				session,
				&resource,
				permission.into(),
				token,
			)
		})
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
			(http::Method::POST, ["processes", "attach"]) => {
				self.try_get_process_attach_stream_request(session, request)
					.await
			},
			(http::Method::POST, ["processes", "spawn"]) => {
				self.try_spawn_process_request(session, request).await
			},
			(http::Method::GET, ["processes", process, "metadata"]) => {
				self.try_get_process_metadata_request(session, request, process)
					.await
			},
			(http::Method::GET, ["processes", process, "availability"]) => {
				self.try_get_process_availability_request(session, request, process)
					.await
			},
			(http::Method::GET, ["processes", process]) => {
				self.try_get_process_request(session, request, process)
					.await
			},
			(http::Method::PUT, ["processes", process]) => {
				self.put_process_request(session, request, process).await
			},
			(http::Method::POST, ["processes", process, "cancel"]) => {
				self.try_cancel_process_request(session, request, process)
					.await
			},
			(http::Method::POST, ["processes", "control"]) => {
				self.try_get_process_control_stream_request(session, request)
					.await
			},
			(http::Method::POST, ["processes", process, "signal"]) => {
				self.try_signal_process_request(session, request, process)
					.await
			},
			(http::Method::GET, ["processes", process, "status"]) => {
				self.try_get_process_status_stream_request(session, request, process)
					.await
			},
			(http::Method::GET, ["processes", process, "children"]) => {
				self.try_get_process_children_stream_request(session, request, process)
					.await
			},
			(http::Method::PUT, ["processes", process, "tty", "size"]) => {
				self.try_set_process_tty_size_request(session, request, process)
					.await
			},
			(http::Method::POST, ["processes", process, "stdio", "read"]) => {
				self.try_read_process_stdio_request(session, request, process)
					.await
			},
			(http::Method::POST, ["processes", process, "stdio", "write"]) => {
				self.try_write_process_stdio_request(session, request, process)
					.await
			},
			(http::Method::POST, ["processes", process, "touch"]) => {
				self.try_touch_process_request(session, request, process)
					.await
			},
			(http::Method::POST, ["processes", process, "wait"]) => {
				self.try_wait_process_future_request(session, request, process)
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
	fn stop<'a>(&'a self, _server: &'a Server) -> BoxFuture<'a, ()> {
		async move {
			// Abort the remote put tasks.
			self.remote_put_tasks.abort_all();
			self.remote_put_tasks.wait().await;
			tracing::trace!("remote process put tasks");

			// Stop the attach client and drain the attach service tasks.
			self.attach.client.stop();
			self.attach.service_tasks.stop_all();
			self.attach.service_tasks.wait().await;

			// Stop the control client and drain the control service tasks.
			self.control.client.stop();
			self.control.service_tasks.stop_all();
			self.control.service_tasks.wait().await;
		}
		.boxed()
	}

	fn abort(&self) {
		self.attach.service_tasks.abort_all();
		self.control.service_tasks.abort_all();
		self.remote_put_tasks.abort_all();
	}
}

impl tangram_server_core::process::Component for Component {
	fn try_get_process_local<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		metadata: bool,
		availability: bool,
		tokens: &'a [tg::authorization::Token],
		source: tg::process::Source,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::get::Output>>> {
		self.try_get_process_local(session, id, metadata, availability, tokens, source)
			.boxed()
	}

	fn authorize_with_process_control_service<'a>(
		&'a self,
		_session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		principal: &'a tg::Principal,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.authorize_with_process_control_service(id, principal)
			.boxed()
	}

	fn create_process_sandbox_permission_arg<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		process: &'a tg::process::Id,
		sandbox: &'a tg::sandbox::Id,
		created_at: i64,
	) -> tg::Result<tangram_index::permission::put::Arg> {
		Self::create_process_sandbox_permission_arg(session, process, sandbox, created_at)
	}

	fn expire_process_log_cache<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		data: &'a tg::process::Data,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.expire_process_log_cache(session, id, data).boxed()
	}

	fn get_process_local<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		metadata: bool,
	) -> BoxFuture<'a, tg::Result<tg::process::get::Output>> {
		self.get_process_local(session, id, metadata).boxed()
	}

	fn notify_process_log(&self, server: &tangram_server_core::Server, id: &tg::process::Id) {
		Self::notify_process_log(server, id);
	}

	fn push_process_objects<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: PushProcessObjectsArg,
	) -> BoxFuture<'a, tg::Result<PushProcessObjectsOutput>> {
		self.push_process_objects(session, arg).boxed()
	}

	fn put_process_local<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		arg: tg::process::put::Arg,
		options: Options,
	) -> BoxFuture<'a, tg::Result<tg::process::put::Output>> {
		self.put_process_local(session, id, arg, options).boxed()
	}

	fn put_process_local_inner<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		arg: tg::process::put::Arg,
		principal: tg::Principal,
		options: Options,
	) -> BoxFuture<'a, tg::Result<tg::process::put::Output>> {
		self.put_process_local_inner(session, id, arg, principal, options)
			.boxed()
	}

	fn request_process_control<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		arg: tg::process::control::ServerRequestArg,
		options: tangram_server_core::control::Options,
	) -> BoxFuture<'a, tg::Result<tg::Result<tg::process::control::ClientResponseOutput>>> {
		self.request_process_control(session, id, arg, options)
			.boxed()
	}

	fn set_process_children_from_index<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		children_set: bool,
		data: &'a mut tg::process::Data,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.set_process_children_from_index(session, id, children_set, data)
			.boxed()
	}

	fn spawn_process_finish_tasks<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
	) {
		self.spawn_process_finish_tasks(session, id);
	}

	fn spawn_publish_process_status_task(
		&self,
		server: &tangram_server_core::Server,
		id: &tg::process::Id,
	) {
		Self::spawn_publish_process_status_task(server, id);
	}

	fn store_process_error<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		error: tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>,
	) -> BoxFuture<'a, tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>> {
		self.store_process_error(session, error).boxed()
	}

	fn try_acquire_scheduled_sandbox_capacity<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		borrowed: bool,
		parent: Option<&'a tg::sandbox::Id>,
		requested: tg::runner::Capacity,
	) -> Option<tangram_server_runner_core::capacity::Allocation> {
		Self::try_acquire_scheduled_sandbox_capacity(session, borrowed, parent, requested)
	}

	fn try_get_process_local_inner<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		metadata: bool,
		source: tg::process::Source,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::get::Output>>> {
		self.try_get_process_local_inner(session, id, metadata, source)
			.boxed()
	}

	fn try_get_process_local_with_permissions<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		permissions: tg::authorization::permission::Set,
		metadata: bool,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::get::Output>>> {
		self.try_get_process_local_with_permissions(session, id, permissions, metadata)
			.boxed()
	}

	fn try_get_process_parent_from_control_service<'a>(
		&'a self,
		_session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::Id>>> {
		self.try_get_process_parent_from_control_service(id).boxed()
	}

	fn try_get_process_regions<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tangram_server_core::process::TryGetProcessRegionsArg<'a>,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::get::Output>>> {
		self.try_get_process_regions(session, arg).boxed()
	}

	fn try_get_process_with_data_from_index<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
	) -> BoxFuture<
		'a,
		tg::Result<Option<(tangram_index::process::Process, Option<tg::process::Data>)>>,
	> {
		self.try_get_process_with_data_from_index(session, id)
			.boxed()
	}

	fn try_resolve_process_command_sync<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		nonce: &'a tg::process::Nonce,
	) -> BoxFuture<'a, tg::Result<Option<tg::Referent<tg::sync::Id>>>> {
		self.try_resolve_process_command_sync(session, nonce)
			.boxed()
	}

	fn try_resolve_process_control_sync<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		kind: tg::sync::join::Kind,
	) -> BoxFuture<'a, tg::Result<Option<tg::Referent<tg::sync::Id>>>> {
		self.try_resolve_process_control_sync(session, id, kind)
			.boxed()
	}

	fn try_get_process_attach_stream<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::process::attach::Arg,
		input: BoxStream<'static, tg::Result<tg::process::attach::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				Option<(
					tg::process::attach::Header,
					BoxStream<'static, tg::Result<tg::process::attach::ServerMessage>>,
				)>,
				tg::Error,
			>,
		>,
	> {
		self.try_get_process_attach_stream(session, arg, input)
			.boxed()
	}

	fn try_spawn_process<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::process::spawn::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<
			BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>,
		>,
	> {
		async move {
			let output = self.try_spawn_process(session, arg).await?;
			Ok(output.boxed())
		}
		.boxed()
	}

	fn try_get_process_metadata<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::Metadata>>> {
		self.try_get_process_metadata(session, id, arg).boxed()
	}

	fn try_get_process_availability<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		arg: tg::process::availability::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::Availability>>> {
		self.try_get_process_availability(session, id, arg).boxed()
	}

	fn try_get_process<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		arg: tg::process::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::get::Output>>> {
		self.try_get_process(session, id, arg).boxed()
	}

	fn put_process<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		arg: tg::process::put::Arg,
	) -> BoxFuture<'a, tg::Result<tg::process::put::Output>> {
		self.put_process(session, id, arg).boxed()
	}

	fn try_cancel_process<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::cancel::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::cancel::Output>>> {
		self.try_cancel_process(session, process, arg).boxed()
	}

	fn try_get_process_control_stream<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
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
	> {
		async move {
			let output = self
				.try_get_process_control_stream_with_context(session, arg, stream)
				.boxed()
				.await?;
			let output =
				output.map_break(|output| output.map(|(header, stream)| (header, stream.boxed())));
			Ok(output)
		}
		.boxed()
	}

	fn try_signal_process<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::signal::post::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_post_process_signal(session, process, arg).boxed()
	}

	fn try_get_process_status_stream<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::status::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>>>
	{
		async move {
			let output = self
				.try_get_process_status_stream(session, process, arg)
				.await?;
			let output = output.map(futures::StreamExt::boxed);
			Ok(output)
		}
		.boxed()
	}

	fn try_get_process_children_stream<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::children::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>>,
	> {
		async move {
			let output = self
				.try_get_process_children_stream(session, process, arg)
				.await?;
			let output = output.map(futures::StreamExt::boxed);
			Ok(output)
		}
		.boxed()
	}

	fn try_set_process_tty_size<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::tty::size::put::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_set_process_tty_size(session, process, arg).boxed()
	}

	fn try_read_process_stdio<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
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
	> {
		async move {
			let output = self
				.try_read_process_stdio(session, process, arg, input)
				.await?;
			let output = output.map_break(|output| output.map(futures::StreamExt::boxed));
			Ok(output)
		}
		.boxed()
	}

	fn try_write_process_stdio<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
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
	> {
		async move {
			let output = self
				.try_write_process_stdio(session, process, arg, input)
				.await?;
			let output = output.map_break(|output| output.map(futures::StreamExt::boxed));
			Ok(output)
		}
		.boxed()
	}

	fn try_touch_process<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_touch_process(session, id, arg).boxed()
	}

	fn try_wait_process_future<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		process: &'a tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::wait::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>>>,
	> {
		async move {
			let output = self.try_wait_process_future(session, process, arg).await?;
			let output = output.map(futures::FutureExt::boxed);
			Ok(output)
		}
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
