use {
	super::{Graph, ProcessNode, UpdateObjectLocalArg, UpdateProcessLocalArg},
	tangram_client::prelude::*,
};

#[test]
fn inline_command_objects_are_distinct_process_children() {
	for count in [0, 2] {
		let id = tg::process::Id::new();
		let command_id = tg::command::Id::new(b"unused");
		let mut data = process_data(&command_id, Some(&[]));
		let objects = (0..count)
			.map(|index| tg::object::Id::from(tg::blob::Id::new(&[index])))
			.collect::<Vec<_>>();
		let args = objects
			.iter()
			.cycle()
			.take(usize::from(count) * 2)
			.map(|id| {
				tg::command::data::Value::Value(tg::value::Data::Object(tg::Referent::with_node(
					id.clone(),
				)))
			})
			.collect();
		let executable = tg::command::data::Executable {
			artifact: None,
			path: Some("true".into()),
		};
		let command = tg::process::data::Command {
			args,
			cwd: None,
			env: std::collections::BTreeMap::default(),
			executable: tg::Referent::with_node(executable),
			host: "test".to_owned(),
			stdin: None,
			user: None,
		};
		let command_id = command.id().unwrap();
		data.command = tg::Referent::with_node(tg::Either::Left(Box::new(command)));
		let arg = tg::sync::Arg::default();
		let mut graph = Graph::new(&arg, false);
		update_process(&mut graph, &id, &data);
		let process = graph
			.nodes()
			.get(&tg::Id::from(id))
			.unwrap()
			.unwrap_process_ref();
		assert_eq!(process.objects().unwrap().len(), usize::from(count));
		assert!(!graph.nodes().contains_key(&tg::Id::from(command_id)));
		for object in objects {
			assert!(graph.nodes().contains_key(&tg::Id::from(object)));
		}
	}
}

#[test]
fn received_object_is_not_stored_until_written() {
	let id = tg::object::Id::from(tg::blob::Id::new(b"pending"));
	let data = tg::object::Data::Blob(tg::blob::Data::Leaf(tg::blob::data::Leaf {
		bytes: b"pending".as_slice().into(),
	}));
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	graph.insert_local_root(id.clone().into());
	let update = UpdateObjectLocalArg {
		data: Some(&data),
		id: &id,
		marked: None,
		metadata: None,
		permissions: Some(tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::SUBTREE,
		)),
		put: None,
		requested: None,
		storage: None,
	};
	graph.update_object_local(update);
	assert!(!graph.get_object_local_availability(&id).subtree);
	assert!(!graph.end_local());
	let update = UpdateObjectLocalArg {
		data: None,
		id: &id,
		marked: Some(true),
		metadata: None,
		permissions: None,
		put: None,
		requested: None,
		storage: Some(tg::object::Storage::default()),
	};
	graph.update_object_local(update);
	assert!(graph.get_object_local_availability(&id).subtree);
	assert!(graph.end_local());
}

#[test]
fn lifted_sync_tokens_follow_attachment_ancestors() {
	let ids = [
		b"directory".as_slice(),
		b"first",
		b"second",
		b"first contents",
		b"second contents",
	]
	.map(|seed| tg::object::Id::from(tg::blob::Id::new(seed)));
	let key =
		tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
			.unwrap();
	let mut tokens = (0..3)
		.map(|_| {
			let body = tg::authorization::Body {
				expires_at: i64::MAX,
				permissions: vec![tg::authorization::Permission::Sync(
					tg::authorization::permission::sync::Permission::Read,
				)],
				resource: tg::sync::Id::new().into(),
			};
			tg::authorization::Token::sign(body, &key).unwrap()
		})
		.collect::<Vec<_>>();
	tokens.sort_by_cached_key(ToString::to_string);
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	update_object(&mut graph, &ids[0], &ids[1..3]);
	update_object(&mut graph, &ids[1], &ids[3..4]);
	update_object(&mut graph, &ids[2], &ids[4..5]);
	let entry = tg::tokens::Entry {
		authorization: tokens.clone(),
	};
	graph.update_object_tokens(&ids[0], &entry, &entry);
	for id in &ids {
		assert_eq!(
			graph
				.get_node_local_tokens(&id.clone().into())
				.authorization,
			tokens
		);
		assert_eq!(
			graph
				.get_node_remote_tokens(&id.clone().into())
				.authorization,
			tokens
		);
	}
	let mut graph = Graph::new(&arg, false);
	update_object(&mut graph, &ids[1], &ids[3..4]);
	update_object(&mut graph, &ids[2], &ids[4..5]);
	graph.update_object_tokens(&ids[1], &entry, &entry);
	graph.update_object_tokens(&ids[3], &entry, &entry);
	for id in [&ids[1], &ids[3]] {
		assert_eq!(
			graph
				.get_node_local_tokens(&id.clone().into())
				.authorization,
			tokens
		);
		assert_eq!(
			graph
				.get_node_remote_tokens(&id.clone().into())
				.authorization,
			tokens
		);
	}
	for id in [&ids[2], &ids[4]] {
		assert!(
			graph
				.get_node_local_tokens(&id.clone().into())
				.authorization
				.is_empty()
		);
		assert!(
			graph
				.get_node_remote_tokens(&id.clone().into())
				.authorization
				.is_empty()
		);
	}
}

#[test]
fn object_facts_settle_in_every_arrival_order() {
	// Create a diamond with a shared dependency.
	let ids = [b"root".as_slice(), b"left", b"right", b"shared"]
		.map(|seed| tg::object::Id::from(tg::blob::Id::new(seed)));
	let children = [
		vec![ids[1].clone(), ids[2].clone()],
		vec![ids[3].clone()],
		vec![ids[3].clone()],
		vec![],
	];
	let permissions = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::SUBTREE,
	);

	// Compare every update and permission arrival order with the reference.
	for order in permutations([0, 1, 2, 3, 4]) {
		let arg = tg::sync::Arg::default();
		let mut graph = Graph::new(&arg, false);
		graph.insert_local_root(ids[0].clone().into());
		let mut granted = false;
		for index in order {
			if index == ids.len() {
				graph.update_object_local_permissions(&ids[0], permissions);
				granted = true;
			} else {
				update_object(&mut graph, &ids[index], &children[index]);
			}
			assert_object_reference(&graph, &ids[0], granted);
		}
		assert!(graph.end_local());
		for id in &ids {
			assert!(graph.object_local_permissions(id).contains(permissions));
			assert!(graph.get_object_local_availability(id).subtree);
		}
		let root = graph.nodes()[&tg::Id::from(ids[0].clone())].unwrap_object_ref();
		let metadata = &root.metadata().unwrap().subtree;
		assert_eq!(metadata.count, Some(5));
		assert_eq!(metadata.depth, Some(3));
		assert_eq!(metadata.size, Some(5));
		assert_eq!(metadata.solvable, Some(false));
		assert_eq!(metadata.solved, Some(true));

		// Repeated facts and edges must not count a shared dependency again.
		for index in order.into_iter().filter(|index| *index < ids.len()) {
			update_object(&mut graph, &ids[index], &children[index]);
			assert_object_reference(&graph, &ids[0], granted);
		}
		let root = graph.nodes()[&tg::Id::from(ids[0].clone())].unwrap_object_ref();
		assert_eq!(root.metadata().unwrap().subtree.count, Some(5));
	}
}

#[test]
fn process_facts_match_recomputation_in_every_arrival_order() {
	// Create the processes and their shared objects in all four aspects.
	let parent = tg::process::Id::new();
	let child = tg::process::Id::new();
	let command = tg::command::Id::new(b"command");
	let error = tg::error::Id::new(b"error");
	let log = tg::blob::Id::new(b"log");
	let output = tg::blob::Id::new(b"output");
	let command_data = tg::object::Data::Command(
		serde_json::from_value(serde_json::json!({"executable": {}, "host": "test"})).unwrap(),
	);
	let error_data = tg::object::Data::Error(tg::error::Data::default());
	let mut parent_data = process_data(&command, Some(std::slice::from_ref(&child)));
	parent_data.error = Some(tg::Either::Right(tg::Referent::with_node(error.clone())));
	parent_data.log = Some(tg::Referent::with_node(log.clone()));
	parent_data.output = Some(tg::value::Data::Object(tg::Referent::with_node(
		output.clone().into(),
	)));
	let child_data = parent_data.clone();
	let child_data = tg::process::Data {
		children: Some(vec![]),
		..child_data
	};

	// Compare the intermediate states for every data arrival order.
	for order in permutations([0, 1, 2, 3, 4, 5]) {
		let arg = tg::sync::Arg::default();
		let mut graph = Graph::new(&arg, false);
		for event in order {
			match event {
				0 => update_process(&mut graph, &parent, &parent_data),
				1 => update_process(&mut graph, &child, &child_data),
				2 => update_object_data(&mut graph, &command.clone().into(), &command_data),
				3 => update_object_data(&mut graph, &error.clone().into(), &error_data),
				4 => update_object(&mut graph, &log.clone().into(), &[]),
				5 => update_object(&mut graph, &output.clone().into(), &[]),
				_ => unreachable!(),
			}
			for id in [&parent, &child] {
				let Some(node) = graph.nodes().get(&tg::Id::from(id.clone())) else {
					continue;
				};
				let node = node.unwrap_process_ref();
				let (metadata, storage) = reference_process(&graph, node);
				assert_eq!(
					node.metadata(),
					metadata.as_ref(),
					"order {order:?}, event {event}"
				);
				assert_eq!(node.local_storage().cloned().unwrap_or_default(), storage);
				assert_eq!(
					serde_json::to_value(graph.get_process_local_availability(id)).unwrap(),
					serde_json::to_value(storage).unwrap()
				);
			}
		}
	}
}

#[cfg(debug_assertions)]
#[test]
fn dependency_facts_reject_revisions() {
	for revised in [None, Some(2)] {
		let old = super::state::Facts {
			metadata: tg::object::metadata::Subtree {
				count: Some(1),
				..Default::default()
			},
			..Default::default()
		};
		let new = super::state::Facts {
			metadata: tg::object::metadata::Subtree {
				count: revised,
				..Default::default()
			},
			..Default::default()
		};
		assert!(
			std::panic::catch_unwind(|| {
				let mut dependencies = super::state::Dependencies::default();
				dependencies.insert(&old);
				dependencies.update(&old, &new);
			})
			.is_err()
		);
	}
	for old in [
		super::state::Facts {
			permissions: true,
			..Default::default()
		},
		super::state::Facts {
			storage: true,
			..Default::default()
		},
	] {
		assert!(
			std::panic::catch_unwind(|| {
				let mut dependencies = super::state::Dependencies::default();
				dependencies.insert(&old);
				dependencies.update(&old, &super::state::Facts::default());
			})
			.is_err()
		);
	}
}

#[test]
fn remote_edges_inherit_existing_permissions_without_local_data() {
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let parent = tg::object::Id::from(tg::blob::Id::new(b"parent"));
	let child = tg::object::Id::from(tg::blob::Id::new(b"child"));
	let permissions = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::SUBTREE,
	);
	graph.update_object_local_permissions(&parent, permissions);
	graph.update_object_remote(false, &child, Some(parent.clone().into()), None, None);
	assert!(graph.object_local_permissions(&child).contains(permissions));
	assert!(!graph.get_object_local_availability(&child).subtree);
	update_object(&mut graph, &child, &[]);
	update_object(&mut graph, &parent, std::slice::from_ref(&child));
	let parent = graph.nodes()[&tg::Id::from(parent)].unwrap_object_ref();
	assert_eq!(parent.metadata().unwrap().subtree.count, Some(2));
	assert!(parent.local_availability().unwrap().subtree);
}

#[test]
fn object_metadata_waits_for_each_required_child_fact() {
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let parent = tg::object::Id::from(tg::blob::Id::new(b"parent"));
	let child = tg::object::Id::from(tg::blob::Id::new(b"child"));
	update_object(&mut graph, &parent, std::slice::from_ref(&child));
	assert_eq!(
		graph.nodes()[&tg::Id::from(parent.clone())]
			.unwrap_object_ref()
			.metadata()
			.unwrap()
			.subtree
			.count,
		None
	);
	let metadata = tg::object::Metadata {
		node: tg::object::metadata::Node {
			size: 1,
			solvable: false,
			solved: true,
		},
		subtree: tg::object::metadata::Subtree {
			count: Some(4),
			..Default::default()
		},
	};
	let update = UpdateObjectLocalArg {
		data: None,
		id: &child,
		marked: None,
		metadata: Some(metadata),
		permissions: None,
		put: None,
		requested: None,
		storage: None,
	};
	graph.update_object_local(update);
	let parent = graph.nodes()[&tg::Id::from(parent)].unwrap_object_ref();
	assert_eq!(parent.metadata().unwrap().subtree.count, Some(5));
	assert_eq!(parent.metadata().unwrap().subtree.size, None);
}

#[test]
fn process_permissions_preserve_edge_and_aspect_boundaries() {
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let parent = tg::process::Id::new();
	let child = tg::process::Id::new();
	let command = tg::command::Id::new(b"command");
	let permissions = tg::authorization::permission::Set::Process(
		tg::authorization::permission::process::Set::SUBTREE_LOG,
	);
	graph.update_process_local_permissions(&parent, permissions);
	graph.update_process_remote(false, &child, Some(parent.clone().into()), None);
	let command_object = tg::object::Id::from(command.clone());
	graph.update_object_remote(
		false,
		&command_object,
		Some(parent.clone().into()),
		Some(crate::sync::queue::ObjectKind::Command),
		None,
	);
	let inherited = graph.process_local_permissions(&child);
	assert!(inherited.contains(permissions));
	assert!(inherited.contains(tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::NodeLog
	)));
	assert!(!inherited.contains(tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::NodeOutput
	)));
	assert!(graph.object_local_permissions(&command_object).is_empty());
	let permissions = tg::authorization::permission::Set::Process(
		tg::authorization::permission::process::Set::PARENT,
	);
	graph.update_process_local_permissions(&parent, permissions);
	assert_eq!(
		graph.process_local_permissions(&child),
		tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::all()
		)
	);
	assert!(graph.object_local_permissions(&command_object).is_empty());
}

#[test]
fn process_metadata_and_availability_settle_without_finalization() {
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let parent = tg::process::Id::new();
	let child = tg::process::Id::new();
	let command = tg::command::Id::new(b"command");
	let output = tg::object::Id::from(tg::blob::Id::new(b"output"));
	let mut parent_data = process_data(&command, Some(std::slice::from_ref(&child)));
	parent_data.output = Some(tg::value::Data::Object(tg::Referent::with_node(
		output.clone(),
	)));
	update_process(&mut graph, &parent, &parent_data);
	let child_data = process_data(&command, None);
	update_process(&mut graph, &child, &child_data);
	assert_eq!(
		graph.nodes()[&tg::Id::from(child.clone())]
			.unwrap_process_ref()
			.metadata()
			.unwrap()
			.subtree
			.count,
		None
	);
	let command_data = tg::object::Data::Command(
		serde_json::from_value(serde_json::json!({"executable": {}, "host": "test"})).unwrap(),
	);
	update_object_data(&mut graph, &command.clone().into(), &command_data);
	let availability = graph.get_process_local_availability(&child);
	assert!(availability.node_command);
	assert!(!availability.subtree_command);
	let child_data = process_data(&command, Some(&[]));
	update_process(&mut graph, &child, &child_data);
	update_object(&mut graph, &output, &[]);
	let permissions = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::SUBTREE,
	);
	graph.update_object_local_permissions(&command.into(), permissions);
	graph.update_object_local_permissions(&output, permissions);
	let parent = graph.nodes()[&tg::Id::from(parent)].unwrap_process_ref();
	let metadata = parent.metadata().unwrap();
	assert_eq!(metadata.subtree.count, Some(2));
	assert_eq!(metadata.node.command.count, Some(1));
	assert_eq!(metadata.subtree.command.count, Some(2));
	assert_eq!(metadata.subtree.command.depth, Some(1));
	assert_eq!(metadata.subtree.output.size, Some(1));
	assert!(parent.local_storage().unwrap().subtree_command);
	assert!(parent.local_availability().unwrap().subtree_command);
	assert!(parent.local_availability().unwrap().subtree_output);
}

#[test]
fn process_log_metadata_waits_for_compaction() {
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let parent = tg::process::Id::new();
	let child = tg::process::Id::new();
	let command = tg::command::Id::new(b"command");
	let parent_data = process_data(&command, Some(std::slice::from_ref(&child)));
	update_process(&mut graph, &parent, &parent_data);
	let mut child_data = process_data(&command, Some(&[]));
	child_data.stdout = tg::process::Stdio::Log;
	update_process(&mut graph, &child, &child_data);
	let node = graph.nodes()[&tg::Id::from(child.clone())].unwrap_process_ref();
	assert_eq!(node.metadata().unwrap().node.log.count, None);
	assert!(node.local_storage().unwrap().node_log);
	let node = graph.nodes()[&tg::Id::from(parent.clone())].unwrap_process_ref();
	assert_eq!(node.metadata().unwrap().subtree.log.count, None);
	assert!(node.local_availability().unwrap().subtree_log);

	let log = tg::blob::Id::new(b"log");
	child_data.log = Some(tg::Referent::with_node(log.clone()));
	update_process(&mut graph, &child, &child_data);
	update_object(&mut graph, &log.clone().into(), &[]);
	let permissions = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::SUBTREE,
	);
	graph.update_object_local_permissions(&log.into(), permissions);
	let node = graph.nodes()[&tg::Id::from(parent)].unwrap_process_ref();
	assert_eq!(node.metadata().unwrap().subtree.log.count, Some(1));
	assert!(node.local_availability().unwrap().subtree_log);
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

#[test]
fn control_requirements_are_independent_in_either_order() {
	use tg::sync::control::{
		GetClientRequestArg, GetObjectServerResponseOutput, GetServerResponseOutput,
	};
	for storage_first in [false, true] {
		let config = tg::sync::Arg::default();
		let mut graph = Graph::new(&config, false);
		let node: tg::Id = tg::blob::Id::new(b"independent").into();
		let permissions = tg::authorization::permission::object::Set::SUBTREE;
		let permission_request = GetClientRequestArg {
			node: node.clone(),
			permissions: tg::authorization::permission::Set::Object(permissions),
			storage: None,
		};
		let stored_request = GetClientRequestArg {
			storage: Some(tg::Storage::Object(tg::object::Storage { subtree: true })),
			..permission_request.clone()
		};
		let storage_only_request = GetClientRequestArg {
			permissions: stored_request.permissions.empty_like(),
			..stored_request.clone()
		};
		assert!(
			graph
				.try_get_node_local_control_output(&permission_request)
				.unwrap()
				.is_none()
		);
		for stored in [storage_first, !storage_first] {
			let output = GetServerResponseOutput::Object(GetObjectServerResponseOutput {
				permissions: if stored {
					tg::authorization::permission::object::Set::empty()
				} else {
					permissions
				},
				storage: stored.then_some(tg::object::Storage { subtree: true }),
			});
			graph
				.update_node_local_control_output(&node, &output)
				.unwrap();
			if stored == storage_first {
				assert_eq!(
					graph
						.try_get_node_local_control_output(&storage_only_request)
						.unwrap()
						.is_some(),
					stored
				);
				assert!(
					graph
						.try_get_node_local_control_output(&stored_request)
						.unwrap()
						.is_none()
				);
				assert_eq!(
					graph
						.try_get_node_local_control_output(&permission_request)
						.unwrap()
						.is_some(),
					!stored
				);
				assert_eq!(
					graph.nodes[&node]
						.unwrap_object_ref()
						.local_storage
						.is_some(),
					stored
				);
			}
		}
		assert!(
			graph
				.try_get_node_local_control_output(&stored_request)
				.unwrap()
				.is_some()
		);
		assert!(!graph.nodes[&node].unwrap_object_ref().marked);
		assert!(
			graph
				.try_get_node_local_control_output(&storage_only_request)
				.unwrap()
				.is_some()
		);
	}
}

#[test]
fn process_data_and_permissions_do_not_prove_storage() {
	use tg::sync::control::GetClientRequestArg;
	let config = tg::sync::Arg::default();
	let mut graph = Graph::new(&config, false);
	let id = tg::process::Id::new();
	let command = tg::command::Id::new(b"command");
	let data = process_data(&command, Some(&[]));
	let permissions = tg::authorization::permission::Set::Process(
		tg::authorization::permission::process::Set::all(),
	);
	let update = UpdateProcessLocalArg {
		data: Some(&data),
		id: &id,
		marked: None,
		metadata: None,
		permissions: Some(permissions),
		requested: None,
		storage: None,
	};
	graph.update_process_local(update);
	let request = GetClientRequestArg {
		node: id.clone().into(),
		permissions,
		storage: None,
	};
	assert!(
		graph
			.try_get_node_local_control_output(&request)
			.unwrap()
			.is_some()
	);
	assert!(graph.get_process_local_storage(&id).is_none());
	let request = GetClientRequestArg {
		storage: Some(tg::Storage::Process(tg::process::Storage::default())),
		..request
	};
	assert!(
		graph
			.try_get_node_local_control_output(&request)
			.unwrap()
			.is_none()
	);
	let update = UpdateProcessLocalArg {
		data: None,
		id: &id,
		marked: None,
		metadata: None,
		permissions: None,
		requested: None,
		storage: Some(tg::process::Storage::default()),
	};
	graph.update_process_local(update);
	assert!(
		graph
			.try_get_node_local_control_output(&request)
			.unwrap()
			.is_some()
	);
	let request = GetClientRequestArg {
		storage: Some(tg::Storage::Process(tg::process::Storage {
			node_command: true,
			..Default::default()
		})),
		..request
	};
	assert!(
		graph
			.try_get_node_local_control_output(&request)
			.unwrap()
			.is_none()
	);
}

#[test]
fn permissions_aggregate_without_storage() {
	use tg::sync::control::{
		GetClientRequestArg, GetObjectServerResponseOutput, GetServerResponseOutput,
	};
	let config = tg::sync::Arg::default();
	let mut graph = Graph::new(&config, false);
	let parent = tg::object::Id::from(tg::blob::Id::new(b"parent"));
	let child = tg::blob::Id::new(b"child");
	let data = tg::object::Data::Blob(tg::blob::Data::Branch(tg::blob::data::Branch {
		children: vec![tg::blob::data::Child {
			blob: child.clone(),
			length: 1,
		}],
	}));
	let update = UpdateObjectLocalArg {
		data: Some(&data),
		id: &parent,
		marked: None,
		metadata: None,
		permissions: Some(tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::NODE,
		)),
		put: None,
		requested: None,
		storage: None,
	};
	graph.update_object_local(update);
	let request = GetClientRequestArg {
		node: parent.clone().into(),
		permissions: tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::SUBTREE,
		),
		storage: None,
	};
	assert!(
		graph
			.try_get_node_local_control_output(&request)
			.unwrap()
			.is_none()
	);
	let output = GetServerResponseOutput::Object(GetObjectServerResponseOutput {
		permissions: tg::authorization::permission::object::Set::SUBTREE,
		storage: None,
	});
	graph
		.update_node_local_control_output(&child.into(), &output)
		.unwrap();
	assert!(
		graph
			.try_get_node_local_control_output(&request)
			.unwrap()
			.is_some()
	);
	assert!(
		graph.nodes[&tg::Id::from(parent)]
			.unwrap_object_ref()
			.local_storage
			.is_none()
	);
}

#[test]
fn process_command_permissions_aggregate_without_process_storage() {
	use tg::authorization::permission::{Set, object, process};
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let parent = tg::process::Id::new();
	let child = tg::process::Id::new();
	let command = tg::command::Id::new(b"command");
	let data = process_data(&command, Some(std::slice::from_ref(&child)));
	let update = UpdateProcessLocalArg {
		data: Some(&data),
		id: &parent,
		marked: None,
		metadata: None,
		permissions: Some(Set::Process(process::Set::NODE)),
		requested: None,
		storage: None,
	};
	graph.update_process_local(update);
	graph.update_object_local_permissions(&command.into(), Set::Object(object::Set::SUBTREE));
	let permissions = Set::Process(process::Set::SUBTREE_COMMAND);
	assert!(
		!graph
			.process_local_permissions(&parent)
			.contains(permissions)
	);
	graph.update_process_local_permissions(&child, permissions);
	assert!(
		graph
			.process_local_permissions(&parent)
			.contains(permissions)
	);
	assert!(
		!graph
			.process_local_permissions(&child)
			.contains(Set::Process(process::Set::NODE))
	);
	assert!(
		!graph
			.process_local_permissions(&parent)
			.contains(Set::Process(process::Set::SUBTREE))
	);
	assert!(
		graph.nodes[&tg::Id::from(parent)]
			.unwrap_process_ref()
			.local_storage()
			.is_none()
	);
}

#[test]
fn control_requests_preserve_storage_variants_on_the_wire() {
	use tg::sync::control::GetClientRequestArg;
	let object: tg::Id = tg::blob::Id::new(b"object").into();
	let process: tg::Id = tg::process::Id::new().into();
	for (node, permissions, storage) in [
		(
			object,
			tg::authorization::permission::Set::Object(
				tg::authorization::permission::object::Set::NODE,
			),
			tg::Storage::Object(tg::object::Storage::default()),
		),
		(
			process,
			tg::authorization::permission::Set::Process(
				tg::authorization::permission::process::Set::NODE,
			),
			tg::Storage::Process(tg::process::Storage::default()),
		),
	] {
		for permissions in [permissions.empty_like(), permissions] {
			for storage in [None, Some(storage.clone())] {
				let request = GetClientRequestArg {
					node: node.clone(),
					permissions,
					storage,
				};
				request.validate().unwrap();
				let json = serde_json::to_value(&request).unwrap();
				let decoded: GetClientRequestArg = serde_json::from_value(json.clone()).unwrap();
				assert_eq!(serde_json::to_value(decoded).unwrap(), json);
				let bytes = tangram_serialize::to_vec(&request).unwrap();
				let decoded: GetClientRequestArg = tangram_serialize::from_slice(&bytes).unwrap();
				assert_eq!(serde_json::to_value(decoded).unwrap(), json);
			}
		}
	}
	let request = GetClientRequestArg {
		node: tg::blob::Id::new(b"mismatched").into(),
		permissions: tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::NODE,
		),
		storage: Some(tg::Storage::Process(tg::process::Storage::default())),
	};
	assert!(request.validate().is_err());
}

fn permutations<const N: usize>(values: [usize; N]) -> Vec<[usize; N]> {
	fn visit<const N: usize>(values: &mut [usize; N], index: usize, output: &mut Vec<[usize; N]>) {
		if index == N {
			output.push(*values);
			return;
		}
		for next in index..N {
			values.swap(index, next);
			visit(values, index + 1, output);
			values.swap(index, next);
		}
	}
	let mut values = values;
	let mut output = Vec::new();
	visit(&mut values, 0, &mut output);
	output
}

fn assert_object_reference(graph: &Graph, root: &tg::object::Id, granted: bool) {
	// Walk the known edges to determine the inherited permissions.
	let mut reachable = std::collections::BTreeSet::new();
	let mut pending = vec![
		graph
			.nodes()
			.get_index_of(&tg::Id::from(root.clone()))
			.unwrap(),
	];
	while let Some(index) = pending.pop() {
		if reachable.insert(index) {
			pending.extend(
				graph
					.nodes()
					.get_index(index)
					.unwrap()
					.1
					.children()
					.into_iter()
					.flatten(),
			);
		}
	}

	// Compare the stored facts with independent subtree walks.
	for (index, (id, node)) in graph.nodes().iter().enumerate() {
		let node = node.unwrap_object_ref();
		let (metadata, stored) = reference_object(graph, index);
		assert_eq!(
			node.metadata()
				.map(|metadata| metadata.subtree.clone())
				.unwrap_or_default(),
			metadata
		);
		assert_eq!(
			node.local_storage().is_some_and(|storage| storage.subtree),
			stored
		);
		assert_eq!(
			node.local_availability()
				.is_some_and(|availability| availability.subtree),
			stored
		);
		let permissions = graph.object_local_permissions(&id.clone().try_into().unwrap());
		assert_eq!(
			!permissions.is_empty(),
			node.marked() || granted && reachable.contains(&index)
		);
		assert_eq!(
			permissions.contains(tg::authorization::permission::Set::Object(
				tg::authorization::permission::object::Set::SUBTREE,
			)),
			stored || granted && reachable.contains(&index)
		);
	}
}

// Recompute from the input node metadata and topology, without consulting any derived fields or counters.
fn reference_object(graph: &Graph, index: usize) -> (tg::object::metadata::Subtree, bool) {
	let node = graph
		.nodes()
		.get_index(index)
		.unwrap()
		.1
		.unwrap_object_ref();
	let Some(children) = node.children() else {
		return (tg::object::metadata::Subtree::default(), false);
	};
	let mut metadata = tg::object::metadata::Subtree {
		count: Some(1),
		depth: Some(1),
		size: node.metadata().map(|metadata| metadata.node.size),
		solvable: node.metadata().map(|metadata| metadata.node.solvable),
		solved: node.metadata().map(|metadata| metadata.node.solved),
	};
	let mut stored = true;
	for child in children {
		let (child, child_stored) = reference_object(graph, *child);
		metadata.count = metadata.count.zip(child.count).map(|(a, b)| a + b);
		metadata.depth = metadata.depth.zip(child.depth).map(|(a, b)| a.max(b + 1));
		metadata.size = metadata.size.zip(child.size).map(|(a, b)| a + b);
		metadata.solvable = metadata.solvable.zip(child.solvable).map(|(a, b)| a || b);
		metadata.solved = metadata.solved.zip(child.solved).map(|(a, b)| a && b);
		stored &= child_stored;
	}

	(metadata, stored)
}

fn reference_process(
	graph: &Graph,
	node: &ProcessNode,
) -> (
	Option<tg::process::Metadata>,
	tangram_index::process::Storage,
) {
	let Some(objects) = node.objects() else {
		return (None, tangram_index::process::Storage::default());
	};
	let mut metadata = tg::process::Metadata::default();
	let mut storage = tangram_index::process::Storage::default();

	// Recompute the child process subtrees.
	let children = node.children().map(|children| {
		children
			.iter()
			.map(|index| {
				reference_process(
					graph,
					graph
						.nodes()
						.get_index(*index)
						.unwrap()
						.1
						.unwrap_process_ref(),
				)
			})
			.collect::<Vec<_>>()
	});
	if let Some(children) = &children {
		metadata.subtree.count = children.iter().try_fold(1, |count, (metadata, _)| {
			Some(count + metadata.as_ref()?.subtree.count?)
		});
		// Preserve the existing sync depth convention.
		metadata.subtree.depth = Some(1);
		storage.subtree = children.iter().all(|(_, storage)| storage.subtree);
	}

	// Aggregate the direct objects and child subtrees for each aspect.
	for kind in [
		tangram_index::process::object::Kind::Command,
		tangram_index::process::object::Kind::Error,
		tangram_index::process::object::Kind::Log,
		tangram_index::process::object::Kind::Output,
	] {
		let mut direct = tg::object::metadata::Subtree {
			count: Some(0),
			depth: Some(0),
			size: Some(0),
			solvable: None,
			solved: None,
		};
		let mut direct_stored = true;
		for (index, _) in objects
			.iter()
			.filter(|(_, object_kind)| *object_kind == kind)
		{
			let (object, stored) = reference_object(graph, *index);
			add_reference_metadata(&mut direct, &object);
			direct_stored &= stored;
		}
		let mut subtree = direct.clone();
		let mut subtree_stored = direct_stored;
		if let Some(children) = &children {
			for (child_metadata, child_storage) in children {
				let child_metadata = child_metadata.clone().unwrap_or_default();
				let (child, stored) = match kind {
					tangram_index::process::object::Kind::Command => (
						&child_metadata.subtree.command,
						child_storage.subtree_command,
					),
					tangram_index::process::object::Kind::Error => {
						(&child_metadata.subtree.error, child_storage.subtree_error)
					},
					tangram_index::process::object::Kind::Log => {
						(&child_metadata.subtree.log, child_storage.subtree_log)
					},
					tangram_index::process::object::Kind::Output => {
						(&child_metadata.subtree.output, child_storage.subtree_output)
					},
				};
				add_reference_metadata(&mut subtree, child);
				subtree_stored &= stored;
			}
		} else {
			subtree = tg::object::metadata::Subtree::default();
			subtree_stored = false;
		}
		let (node_metadata, subtree_metadata, node_storage, subtree_storage) = match kind {
			tangram_index::process::object::Kind::Command => (
				&mut metadata.node.command,
				&mut metadata.subtree.command,
				&mut storage.node_command,
				&mut storage.subtree_command,
			),
			tangram_index::process::object::Kind::Error => (
				&mut metadata.node.error,
				&mut metadata.subtree.error,
				&mut storage.node_error,
				&mut storage.subtree_error,
			),
			tangram_index::process::object::Kind::Log => (
				&mut metadata.node.log,
				&mut metadata.subtree.log,
				&mut storage.node_log,
				&mut storage.subtree_log,
			),
			tangram_index::process::object::Kind::Output => (
				&mut metadata.node.output,
				&mut metadata.subtree.output,
				&mut storage.node_output,
				&mut storage.subtree_output,
			),
		};
		*node_metadata = direct;
		*subtree_metadata = subtree;
		*node_storage = direct_stored;
		*subtree_storage = subtree_stored;
	}

	(Some(metadata), storage)
}

fn add_reference_metadata(
	total: &mut tg::object::metadata::Subtree,
	metadata: &tg::object::metadata::Subtree,
) {
	total.count = total.count.zip(metadata.count).map(|(a, b)| a + b);
	total.depth = total.depth.zip(metadata.depth).map(|(a, b)| a.max(b));
	total.size = total.size.zip(metadata.size).map(|(a, b)| a + b);
}

fn update_object(graph: &mut Graph, id: &tg::object::Id, children: &[tg::object::Id]) {
	let children = children
		.iter()
		.map(|id| tg::blob::data::Child {
			blob: id.clone().try_into().unwrap(),
			length: 1,
		})
		.collect();
	let data = tg::object::Data::Blob(tg::blob::Data::Branch(tg::blob::data::Branch { children }));
	update_object_data(graph, id, &data);
}

fn update_object_data(graph: &mut Graph, id: &tg::object::Id, data: &tg::object::Data) {
	let metadata = tg::object::Metadata {
		node: tg::object::metadata::Node {
			size: 1,
			solvable: false,
			solved: true,
		},
		subtree: tg::object::metadata::Subtree::default(),
	};
	let update = UpdateObjectLocalArg {
		data: Some(data),
		id,
		marked: Some(true),
		metadata: Some(metadata),
		permissions: None,
		put: None,
		requested: None,
		storage: Some(tg::object::Storage::default()),
	};
	graph.update_object_local(update);
}

fn process_data(
	command: &tg::command::Id,
	children: Option<&[tg::process::Id]>,
) -> tg::process::Data {
	let children = children.map(|children| {
		children
			.iter()
			.map(|child| serde_json::json!({"process": child.to_string()}))
			.collect::<Vec<_>>()
	});
	serde_json::from_value(serde_json::json!({
		"children": children,
		"command": {"node": command.to_string()},
		"created_at": 0,
		"host": "test",
		"sandbox": tg::sandbox::Id::new().to_string(),
		"status": "finished"
	}))
	.unwrap()
}

fn update_process(graph: &mut Graph, id: &tg::process::Id, data: &tg::process::Data) {
	let update = UpdateProcessLocalArg {
		data: Some(data),
		id,
		marked: Some(true),
		metadata: None,
		permissions: None,
		requested: None,
		storage: Some(tg::process::Storage::default()),
	};
	graph.update_process_local(update);
}
