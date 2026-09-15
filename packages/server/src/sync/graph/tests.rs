use {
	super::{Graph, Node, ObjectNode, ProcessNode},
	tangram_client::prelude::*,
};

#[test]
fn object_grants_retain_proven_subtree_before_storage() {
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let id = tg::Id::from(tg::file::Id::new(b"object"));
	let subtree = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::SUBTREE,
	);
	let node = ObjectNode {
		local_permissions: Some(subtree),
		local_storage: Some(tangram_index::object::Storage { subtree: false }),
		..Default::default()
	};
	graph.nodes.insert(id.clone(), Node::Object(node));
	assert!(graph.try_get_node_local_grant_permissions(&id).is_none());
	graph.nodes[&id].unwrap_object_mut().marked = true;
	let permissions = graph.try_get_node_local_grant_permissions(&id).unwrap();
	assert!(permissions.contains(subtree));
	graph.nodes[&id].unwrap_object_mut().local_permissions = None;
	let permissions = graph.try_get_node_local_grant_permissions(&id).unwrap();
	assert!(!permissions.contains(subtree));
}

#[test]
fn process_grants_combine_storage_and_existing_proofs() {
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let id = tg::Id::from(tg::process::Id::new());
	let mut proven = tg::authorization::permission::process::Set::NODE_ERROR;
	proven.insert(tg::authorization::permission::process::Set::SUBTREE_OUTPUT);
	let node = ProcessNode {
		local_availability: Some(tg::process::Availability {
			node_log: true,
			..Default::default()
		}),
		local_permissions: Some(tg::authorization::permission::Set::Process(proven)),
		marked: true,
		..Default::default()
	};
	graph.nodes.insert(id.clone(), Node::Process(node));
	let mut expected = proven;
	expected.insert(tg::authorization::permission::process::Set::NODE);
	expected.insert(tg::authorization::permission::process::Set::NODE_LOG);
	assert_eq!(
		graph.try_get_node_local_grant_permissions(&id),
		Some(tg::authorization::permission::Set::Process(expected))
	);
}

#[test]
fn object_control_updates_keep_storage_and_permissions_independent() {
	use tg::sync::control::{GetObjectServerResponseOutput, GetServerResponseOutput};
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let id = tg::object::Id::from(tg::file::Id::new(b"object"));
	let output = GetServerResponseOutput::Object(GetObjectServerResponseOutput {
		permissions: tg::authorization::permission::object::Set::SUBTREE,
		storage: Some(tg::object::Storage { subtree: false }),
	});
	graph
		.update_node_local_control_output(&id.clone().into(), &output)
		.unwrap();
	assert!(!graph.get_object_local_availability(&id).subtree);

	let output = GetServerResponseOutput::Object(GetObjectServerResponseOutput {
		permissions: tg::authorization::permission::object::Set::empty(),
		storage: Some(tg::object::Storage { subtree: true }),
	});
	graph
		.update_node_local_control_output(&id.clone().into(), &output)
		.unwrap();
	assert!(graph.get_object_local_availability(&id).subtree);

	let output = GetServerResponseOutput::Object(GetObjectServerResponseOutput {
		permissions: tg::authorization::permission::object::Set::empty(),
		storage: None,
	});
	graph
		.update_node_local_control_output(&id.clone().into(), &output)
		.unwrap();
	assert!(graph.get_object_local_availability(&id).subtree);
}

#[test]
fn process_control_updates_merge_individual_fields() {
	use tg::sync::control::{GetProcessServerResponseOutput, GetServerResponseOutput};
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let id = tg::process::Id::new();
	let output = GetServerResponseOutput::Process(GetProcessServerResponseOutput {
		permissions: tg::authorization::permission::process::Set::NODE_LOG,
		storage: Some(tg::process::Storage {
			node_log: true,
			node_output: true,
			..Default::default()
		}),
	});
	graph
		.update_node_local_control_output(&id.clone().into(), &output)
		.unwrap();
	let availability = graph.get_process_local_availability(&id);
	assert!(availability.node_log);
	assert!(!availability.node_output);

	let output = GetServerResponseOutput::Process(GetProcessServerResponseOutput {
		permissions: tg::authorization::permission::process::Set::NODE_ERROR,
		storage: Some(tg::process::Storage {
			node_error: true,
			..Default::default()
		}),
	});
	graph
		.update_node_local_control_output(&id.clone().into(), &output)
		.unwrap();
	let availability = graph.get_process_local_availability(&id);
	assert!(availability.node_error);
	assert!(availability.node_log);
	assert!(!availability.node_output);
	assert!(graph.get_process_local_storage(&id).unwrap().node_output);
}

#[test]
fn control_responses_preserve_storage_and_permissions_on_the_wire() {
	use tg::sync::control::{
		GetObjectServerResponseOutput, GetProcessServerResponseOutput, GetServerResponseOutput,
	};
	let outputs = [
		GetServerResponseOutput::Object(GetObjectServerResponseOutput {
			permissions: tg::authorization::permission::object::Set::empty(),
			storage: None,
		}),
		GetServerResponseOutput::Object(GetObjectServerResponseOutput {
			permissions: tg::authorization::permission::object::Set::SUBTREE,
			storage: Some(tg::object::Storage::default()),
		}),
		GetServerResponseOutput::Process(GetProcessServerResponseOutput {
			permissions: tg::authorization::permission::process::Set::all(),
			storage: Some(tg::process::Storage {
				node_command: true,
				node_error: true,
				subtree_log: true,
				subtree_output: true,
				..Default::default()
			}),
		}),
	];
	for output in outputs {
		let json = serde_json::to_value(&output).unwrap();
		let decoded: GetServerResponseOutput = serde_json::from_value(json.clone()).unwrap();
		assert_eq!(serde_json::to_value(decoded).unwrap(), json);
		let bytes = tangram_serialize::to_vec(&output).unwrap();
		let decoded: GetServerResponseOutput = tangram_serialize::from_slice(&bytes).unwrap();
		assert_eq!(serde_json::to_value(decoded).unwrap(), json);
	}
}
