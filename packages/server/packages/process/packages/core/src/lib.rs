use {
	dashmap::DashMap, indexmap::IndexMap, std::collections::BTreeSet, tangram_client::prelude::*,
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
