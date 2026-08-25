use {
	crate::{Server, Session},
	dashmap::DashMap,
	futures::future::BoxFuture,
	indexmap::IndexMap,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_messenger::prelude::*,
};

mod grant;

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

pub(crate) type ConnectionFuture = BoxFuture<'static, tg::Result<control::Connected>>;
pub type IndexTask = tangram_futures::task::Shared<tg::Result<()>>;
pub type Map = DashMap<tg::process::Id, tg::sandbox::Id, tg::id::BuildHasher>;

#[derive(Default)]
pub struct Processes {
	indexes: DashMap<tg::process::Id, u64, tg::id::BuildHasher>,
	processes: DashMap<u64, State, fnv::FnvBuildHasher>,
}

pub struct Child {
	pub data: tg::process::data::Child,
	pub lease: Option<String>,
	pub location: Option<tg::location::Arg>,
}

pub struct State {
	pub children: IndexMap<tg::process::Id, Child, tg::id::BuildHasher>,
	pub control: tokio::sync::mpsc::Sender<tg::process::control::ClientMessage>,
	pub data: tg::process::Data,
	pub finish: Option<tg::process::control::FinishServerRequestArg>,
	pub id: Option<tg::process::Id>,
	pub id_receiver: tokio::sync::watch::Receiver<Option<tg::process::Id>>,
	pub index_task: IndexTask,
	pub inner_token: Option<String>,
	pub leases: BTreeSet<String>,
	pub process: Option<tangram_sandbox::Process>,
	pub stopper: tangram_futures::task::Stopper,
}

impl Processes {
	#[must_use]
	pub fn get(&self, index: u64) -> Option<dashmap::mapref::one::Ref<'_, u64, State>> {
		self.processes.get(&index)
	}

	#[must_use]
	pub fn get_by_id(
		&self,
		id: &tg::process::Id,
	) -> Option<dashmap::mapref::one::Ref<'_, u64, State>> {
		let index = *self.indexes.get(id)?;

		self.processes.get(&index)
	}

	#[must_use]
	pub fn get_mut(&self, index: u64) -> Option<dashmap::mapref::one::RefMut<'_, u64, State>> {
		self.processes.get_mut(&index)
	}

	#[must_use]
	pub fn get_mut_by_id(
		&self,
		id: &tg::process::Id,
	) -> Option<dashmap::mapref::one::RefMut<'_, u64, State>> {
		let index = *self.indexes.get(id)?;

		self.processes.get_mut(&index)
	}

	#[must_use]
	pub fn ids(&self) -> Vec<tg::process::Id> {
		self.indexes
			.iter()
			.map(|entry| entry.key().clone())
			.collect()
	}

	pub fn insert(&self, index: u64, state: State) {
		assert!(state.id.is_none(), "the process ID is already set");
		match self.processes.entry(index) {
			dashmap::Entry::Occupied(_) => panic!("the process index is already in use"),
			dashmap::Entry::Vacant(entry) => {
				entry.insert(state);
			},
		}
	}

	#[must_use]
	pub fn iter(&self) -> dashmap::iter::Iter<'_, u64, State, fnv::FnvBuildHasher> {
		self.processes.iter()
	}

	#[must_use]
	pub fn iter_mut(&self) -> dashmap::iter::IterMut<'_, u64, State, fnv::FnvBuildHasher> {
		self.processes.iter_mut()
	}

	pub fn remove(&self, index: u64) -> Option<State> {
		let (_, state) = self.processes.remove(&index)?;
		if let Some(id) = &state.id {
			self.indexes.remove(id);
		}

		Some(state)
	}

	pub fn set_id(&self, index: u64, id: tg::process::Id) {
		let mut process = self
			.processes
			.get_mut(&index)
			.expect("the process index was not found");
		assert!(process.id.is_none(), "the process ID is already set");
		match self.indexes.entry(id.clone()) {
			dashmap::Entry::Occupied(_) => panic!("the process ID is already in use"),
			dashmap::Entry::Vacant(entry) => {
				process.id = Some(id);
				entry.insert(index);
			},
		}
	}
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

impl Session {
	pub(crate) fn process_permission_for_data(
		&self,
		data: &tg::process::Data,
	) -> tg::authorization::permission::process::Set {
		let mut permissions = tg::authorization::permission::process::Set::NODE;
		if self.process_children_grant_subtree(data.children.as_deref().unwrap_or_default()) {
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE);
		}
		if self
			.process_output_grants_subtree(data.output.as_ref())
			.unwrap_or(true)
		{
			permissions.insert(tg::authorization::permission::process::Set::NODE_OUTPUT);
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE_OUTPUT);
		}
		if self.process_error_grants_subtree(data.error.as_ref()) {
			permissions.insert(tg::authorization::permission::process::Set::NODE_ERROR);
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE_ERROR);
		}
		if self.process_log_grants_subtree(data.log.as_ref()) {
			permissions.insert(tg::authorization::permission::process::Set::NODE_LOG);
			permissions.insert(tg::authorization::permission::process::Set::SUBTREE_LOG);
		}
		permissions
	}

	fn process_children_grant_subtree(&self, children: &[tg::process::data::Child]) -> bool {
		children
			.iter()
			.all(|child| self.process_token_grants_subtree(&child.process))
	}

	fn process_token_grants_subtree(&self, process: &tg::Referent<tg::process::Id>) -> bool {
		let Some(token) = process.options.tokens.local() else {
			return false;
		};
		let resource = tg::Selector::Id(process.node.clone().into());
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Subtree,
		);
		self.authorize_token(&resource, permission.into(), token)
	}

	fn process_output_grants_subtree(&self, output: Option<&tg::value::Data>) -> Option<bool> {
		output.map(|output| self.value_data_tokens_grant_subtree(output))
	}

	fn process_error_grants_subtree(
		&self,
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
			tg::Either::Right(error) => self.object_token_grants_subtree_for_process(error),
		}
	}

	fn process_log_grants_subtree(&self, log: Option<&tg::Referent<tg::blob::Id>>) -> bool {
		log.is_none_or(|log| self.object_token_grants_subtree_for_process(log))
	}

	fn value_data_tokens_grant_subtree(&self, data: &tg::value::Data) -> bool {
		match data {
			tg::value::Data::Array(array) => array
				.iter()
				.all(|value| self.value_data_tokens_grant_subtree(value)),
			tg::value::Data::Map(map) => map
				.values()
				.all(|value| self.value_data_tokens_grant_subtree(value)),
			tg::value::Data::Mutation(mutation) => {
				self.mutation_data_tokens_grant_subtree(mutation)
			},
			tg::value::Data::Module(module) => {
				let mut children = std::collections::BTreeSet::new();
				module.children(&mut children);
				children.into_iter().all(|id| {
					let object = tg::Referent::with_node_and_tokens(
						id,
						module.referent.options.tokens.clone(),
					);
					self.object_token_grants_subtree_for_process(&object)
				})
			},
			tg::value::Data::Object(object) => self.object_token_grants_subtree_for_process(object),
			tg::value::Data::Template(template) => {
				self.template_data_tokens_grant_subtree(template)
			},
			tg::value::Data::Bool(_)
			| tg::value::Data::Bytes(_)
			| tg::value::Data::Null
			| tg::value::Data::Number(_)
			| tg::value::Data::Placeholder(_)
			| tg::value::Data::String(_) => true,
		}
	}

	fn mutation_data_tokens_grant_subtree(&self, data: &tg::mutation::Data) -> bool {
		match data {
			tg::mutation::Data::Append { values } | tg::mutation::Data::Prepend { values } => {
				values
					.iter()
					.all(|value| self.value_data_tokens_grant_subtree(value))
			},
			tg::mutation::Data::Merge { value } => value
				.values()
				.all(|value| self.value_data_tokens_grant_subtree(value)),
			tg::mutation::Data::Prefix { template, .. }
			| tg::mutation::Data::Suffix { template, .. } => {
				self.template_data_tokens_grant_subtree(template)
			},
			tg::mutation::Data::Set { value } | tg::mutation::Data::SetIfUnset { value } => {
				self.value_data_tokens_grant_subtree(value)
			},
			tg::mutation::Data::Unset => true,
		}
	}

	fn template_data_tokens_grant_subtree(&self, data: &tg::template::Data) -> bool {
		data.components.iter().all(|component| match component {
			tg::template::data::Component::Artifact(artifact) => {
				self.object_token_grants_subtree_for_process(artifact)
			},
			tg::template::data::Component::Placeholder(_)
			| tg::template::data::Component::String(_) => true,
		})
	}

	fn object_token_grants_subtree_for_process<T>(&self, object: &tg::Referent<T>) -> bool
	where
		T: Clone + Into<tg::Id>,
	{
		let Some(token) = object.options.tokens.local() else {
			return false;
		};
		let resource = tg::Selector::Id(object.node.clone().into());
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		self.authorize_token(&resource, permission.into(), token)
	}
}

impl Server {
	pub(crate) fn spawn_publish_process_status_task(&self, id: &tg::process::Id) {
		let subject = format!("processes.{id}.status");
		tokio::spawn({
			let server = self.clone();
			async move {
				let result = server.messenger.publish(subject, ()).await;
				if let Err(error) = result {
					tracing::error!(%error, "failed to publish the process status message");
				}
			}
		});
	}
}
