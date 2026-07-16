use {
	crate::{Server, Session},
	dashmap::DashMap,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_messenger::prelude::*,
};

pub mod cancel;
pub mod children;
pub mod control;
pub mod finalize;
pub mod get;
pub mod metadata;
pub mod put;
pub mod signal;
pub mod spawn;
pub mod status;
pub mod stdio;
pub mod store;
pub mod touch;
pub mod tty;
pub mod wait;

pub type Map = DashMap<tg::process::Id, tg::sandbox::Id, tg::id::BuildHasher>;

pub struct State {
	pub children: Vec<Child>,
	pub control: tokio::sync::mpsc::Sender<tg::process::control::ClientMessage>,
	pub data: tg::process::Data,
	pub finish: Option<tg::process::control::FinishServerRequestArg>,
	pub leases: BTreeSet<String>,
	pub process: Option<tangram_sandbox::Process>,
	pub stopper: tangram_futures::task::Stopper,
	pub token: String,
}

pub struct Child {
	pub lease: String,
	pub location: Option<tg::location::Arg>,
	pub process: tg::process::Id,
}

impl Session {
	pub(crate) fn process_permission_for_data(
		&self,
		data: &tg::process::Data,
	) -> tg::grant::permission::process::Set {
		let mut permissions = tg::grant::permission::process::Set::NODE;
		if self.process_children_grant_subtree(data.children.as_deref().unwrap_or_default()) {
			permissions.insert(tg::grant::permission::process::Set::SUBTREE);
		}
		if self
			.process_output_grants_subtree(data.output.as_ref())
			.unwrap_or(true)
		{
			permissions.insert(tg::grant::permission::process::Set::NODE_OUTPUT);
			permissions.insert(tg::grant::permission::process::Set::SUBTREE_OUTPUT);
		}
		if self.process_error_grants_subtree(data.error.as_ref()) {
			permissions.insert(tg::grant::permission::process::Set::NODE_ERROR);
			permissions.insert(tg::grant::permission::process::Set::SUBTREE_ERROR);
		}
		if self.process_log_grants_subtree(data.log.as_ref()) {
			permissions.insert(tg::grant::permission::process::Set::NODE_LOG);
			permissions.insert(tg::grant::permission::process::Set::SUBTREE_LOG);
		}
		permissions
	}

	fn process_children_grant_subtree(&self, children: &[tg::process::data::Child]) -> bool {
		children
			.iter()
			.all(|child| self.process_token_grants_subtree(&child.process))
	}

	fn process_token_grants_subtree(&self, process: &tg::Referent<tg::process::Id>) -> bool {
		let Some(token) = &process.options.token else {
			return false;
		};
		let resource = tg::grant::Resource::Id(process.item.clone().into());
		let permission =
			tg::grant::Permission::Process(tg::grant::permission::process::Permission::Subtree);
		self.authorize_token(&resource, permission.into(), token)
	}

	fn process_output_grants_subtree(&self, output: Option<&tg::value::Data>) -> Option<bool> {
		output.map(|output| self.value_data_tokens_grant_subtree(output))
	}

	fn process_error_grants_subtree(
		&self,
		error: Option<&tg::Either<tg::error::Data, tg::MaybeWithToken<tg::error::Id>>>,
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

	fn process_log_grants_subtree(&self, log: Option<&tg::MaybeWithToken<tg::blob::Id>>) -> bool {
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

	fn object_token_grants_subtree_for_process<T>(&self, object: &tg::MaybeWithToken<T>) -> bool
	where
		T: Clone + Into<tg::Id>,
	{
		let tg::Either::Right(object) = object else {
			return false;
		};
		let resource = tg::grant::Resource::Id(object.id.clone().into());
		let permission =
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree);
		self.authorize_token(&resource, permission.into(), &object.token)
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
