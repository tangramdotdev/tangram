use tangram_client::prelude::*;

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
