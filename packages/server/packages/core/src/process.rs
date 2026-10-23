use {
	crate::Session,
	dashmap::DashMap,
	futures::{future::BoxFuture, stream::BoxStream},
	indexmap::IndexMap,
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_http::body::Boxed as BoxBody,
};

pub mod control;
pub mod put;

pub type IndexTask = tangram_futures::task::Shared<tg::Result<()>>;

pub type Map = DashMap<tg::process::Id, tg::sandbox::Id, tg::id::BuildHasher>;

pub type Processes = DashMap<tg::process::Id, State, tg::id::BuildHasher>;

pub struct Child {
	pub data: tg::process::data::Child,
	pub lease: Option<tg::process::Lease>,
	pub location: Option<tg::location::Arg>,
}

pub struct State {
	pub changed: tokio::sync::watch::Sender<()>,
	pub children: IndexMap<tg::process::Id, Child, tg::id::BuildHasher>,
	pub control: tg::process::control::Sender,
	pub control_sender: control::local::Local,
	pub data: tg::process::Data,
	pub finish: Option<tg::process::control::FinishServerRequestArg>,
	pub index_task: IndexTask,
	pub inner_token: tokio::sync::watch::Receiver<Option<String>>,
	pub leases: BTreeSet<tg::process::Lease>,
	pub parent: Option<tg::process::Id>,
	pub process: Option<tangram_sandbox::Process>,
	pub stopper: tangram_futures::task::Stopper,
}

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
		options: put::Options,
	) -> BoxFuture<'a, tg::Result<tg::process::put::Output>>;

	fn put_process_local_inner<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		arg: tg::process::put::Arg,
		principal: tg::Principal,
		options: put::Options,
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
	) -> Option<crate::runner::capacity::Allocation>;

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

pub fn update_process_value_tokens(
	data: &mut tg::value::Data,
	update: &mut impl FnMut(&mut tg::authorization::Tokens, &tg::object::Id),
) {
	match data {
		tg::value::Data::Array(array) => {
			for value in array {
				update_process_value_tokens(value, update);
			}
		},
		tg::value::Data::Bool(_)
		| tg::value::Data::Bytes(_)
		| tg::value::Data::Null
		| tg::value::Data::Number(_)
		| tg::value::Data::Placeholder(_)
		| tg::value::Data::String(_) => {},
		tg::value::Data::Map(map) => {
			for value in map.values_mut() {
				update_process_value_tokens(value, update);
			}
		},
		tg::value::Data::Module(module) => {
			let mut objects = std::collections::BTreeSet::new();
			module.children(&mut objects);
			if let Some(id) = objects.first() {
				update(&mut module.referent.options.tokens, id);
			} else {
				module.referent.options.clear_location_and_tokens();
			}
		},
		tg::value::Data::Mutation(mutation) => match mutation {
			tg::mutation::Data::Append { values } | tg::mutation::Data::Prepend { values } => {
				for value in values {
					update_process_value_tokens(value, update);
				}
			},
			tg::mutation::Data::Merge { value } => {
				for value in value.values_mut() {
					update_process_value_tokens(value, update);
				}
			},
			tg::mutation::Data::Prefix { template, .. }
			| tg::mutation::Data::Suffix { template, .. } => {
				update_wait_template_tokens(template, update);
			},
			tg::mutation::Data::Set { value } | tg::mutation::Data::SetIfUnset { value } => {
				update_process_value_tokens(value, update);
			},
			tg::mutation::Data::Unset => {},
		},
		tg::value::Data::Object(object) => update(&mut object.options.tokens, &object.node),
		tg::value::Data::Template(template) => {
			update_wait_template_tokens(template, update);
		},
	}
}

pub fn update_wait_template_tokens(
	template: &mut tg::template::Data,
	update: &mut impl FnMut(&mut tg::authorization::Tokens, &tg::object::Id),
) {
	for component in &mut template.components {
		if let tg::template::data::Component::Artifact(artifact) = component {
			update(&mut artifact.options.tokens, &artifact.node.clone().into());
		}
	}
}

#[must_use]
pub fn mask_process_metadata_with_permissions(
	metadata: &tg::process::Metadata,
	permissions: tg::authorization::permission::Set,
) -> Option<tg::process::Metadata> {
	let mut output = tg::process::Metadata::default();
	let mut authorized = false;

	if permissions.contains(tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Node,
	)) {
		authorized = true;
	}

	if permissions.contains(tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Subtree,
	)) {
		output.subtree.count = metadata.subtree.count;
		authorized = true;
	}

	if permissions.contains(tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::SubtreeCommandObjects,
	)) {
		output.node.command_objects = metadata.node.command_objects.clone();
		output.subtree.command_objects = metadata.subtree.command_objects.clone();
		authorized = true;
	} else if permissions.contains(tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::NodeCommandObjects,
	)) {
		output.node.command_objects = metadata.node.command_objects.clone();
		authorized = true;
	}

	if permissions.contains(tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::SubtreeErrorObjects,
	)) {
		output.node.error_objects = metadata.node.error_objects.clone();
		output.subtree.error_objects = metadata.subtree.error_objects.clone();
		authorized = true;
	} else if permissions.contains(tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::NodeErrorObjects,
	)) {
		output.node.error_objects = metadata.node.error_objects.clone();
		authorized = true;
	}

	if permissions.contains(tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::SubtreeLogObjects,
	)) {
		output.node.log_objects = metadata.node.log_objects.clone();
		output.subtree.log_objects = metadata.subtree.log_objects.clone();
		authorized = true;
	} else if permissions.contains(tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::NodeLogObjects,
	)) {
		output.node.log_objects = metadata.node.log_objects.clone();
		authorized = true;
	}

	if permissions.contains(tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::SubtreeOutputObjects,
	)) {
		output.node.output_objects = metadata.node.output_objects.clone();
		output.subtree.output_objects = metadata.subtree.output_objects.clone();
		authorized = true;
	} else if permissions.contains(tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::NodeOutputObjects,
	)) {
		output.node.output_objects = metadata.node.output_objects.clone();
		authorized = true;
	}

	if !authorized {
		return None;
	}

	Some(output)
}

impl State {
	#[must_use]
	pub fn data(&self) -> tg::process::Data {
		let mut data = self.data.clone();
		data.children = Some(
			self.children
				.values()
				.map(|child| child.data.clone())
				.collect(),
		);

		data
	}
}
