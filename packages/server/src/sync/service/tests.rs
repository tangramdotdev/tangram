use {
	super::Event,
	crate::{
		Server,
		sync::graph::{Graph, UpdateObjectLocalArg},
	},
	std::sync::Mutex,
	tangram_client::prelude::*,
	tg::sync::service as protocol,
};

#[test]
fn empty_requests_are_notified_when_a_child_enters_the_graph() {
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
	graph.set_service(sender.downgrade());
	let parent: tg::object::Id = tg::blob::Id::new(b"parent").into();
	let child = tg::blob::Id::new(b"child");
	let request = protocol::Arg {
		node: child.clone().into(),
		permissions: tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::empty(),
		),
		storage: tg::storage::Set::Object(tg::object::storage::Set::empty()),
	};
	assert!(
		graph
			.try_verify_node_local_service_output(&request)
			.unwrap()
			.is_none()
	);
	let data = tg::object::Data::Blob(tg::blob::Data::Branch(tg::blob::data::Branch {
		children: vec![tg::blob::data::Child {
			blob: child,
			length: 1,
		}],
	}));
	let update = UpdateObjectLocalArg {
		data: Some(&data),
		id: &parent,
		marked: None,
		metadata: None,
		permissions: None,
		put: None,
		requested: None,
		storage: None,
	};
	graph.update_object_local(update);
	let Event::Nodes(nodes) = receiver.try_recv().unwrap() else {
		panic!("expected node notifications")
	};
	assert!(nodes.contains(&request.node));
	let output = graph
		.try_verify_node_local_service_output(&request)
		.unwrap()
		.unwrap();
	assert!(output.permissions().is_empty());
	let protocol::Output::Object(output) = output else {
		panic!("expected an object response")
	};
	assert!(output.storage.is_empty());
}

#[test]
fn requests_wait_for_their_own_requirements() {
	let id: tg::object::Id = tg::blob::Id::new(b"requirements").into();
	let graph = graph();
	let permissions = tg::authorization::permission::object::Set::SUBTREE;
	let permissions_arg =
		protocol::Arg::object(id.clone(), permissions, tg::object::storage::Set::empty());
	let storage_arg = protocol::Arg::object(
		id.clone(),
		permissions,
		tg::object::storage::Set::NODE | tg::object::storage::Set::SUBTREE,
	);
	assert!(Server::sync_service_result(&graph, None, &permissions_arg).is_none());
	assert!(Server::sync_service_result(&graph, None, &storage_arg).is_none());
	graph.lock().unwrap().update_object_local_permissions(
		&id,
		tg::authorization::permission::Set::Object(permissions),
	);
	let output = Server::sync_service_result(&graph, None, &permissions_arg)
		.unwrap()
		.unwrap();
	assert!(matches!(output, Some(protocol::Output::Object(output)) if output.storage.is_empty()));
	assert!(Server::sync_service_result(&graph, None, &storage_arg).is_none());
	let update = UpdateObjectLocalArg {
		data: None,
		id: &id,
		marked: None,
		metadata: None,
		permissions: None,
		put: None,
		requested: None,
		storage: Some(tg::object::storage::Set::NODE | tg::object::storage::Set::SUBTREE),
	};
	graph.lock().unwrap().update_object_local(update);
	assert!(Server::sync_service_result(&graph, None, &storage_arg).is_some());
}

#[test]
fn finished_sync_distinguishes_missing_from_permission_only_success() {
	let id: tg::object::Id = tg::blob::Id::new(b"finished").into();
	for failure in [false, true] {
		let graph = graph();
		let permissions = tg::authorization::permission::object::Set::NODE;
		let permissions_arg =
			protocol::Arg::object(id.clone(), permissions, tg::object::storage::Set::empty());
		let storage_arg =
			protocol::Arg::object(id.clone(), permissions, tg::object::storage::Set::NODE);
		graph.lock().unwrap().update_object_local_permissions(
			&id,
			tg::authorization::permission::Set::Object(permissions),
		);
		let result = if failure {
			Err(tg::error!("the transfer failed"))
		} else {
			Ok(())
		};
		let permissions =
			Server::sync_service_result(&graph, Some(&result), &permissions_arg).unwrap();
		assert!(matches!(permissions, Ok(Some(_))));
		let storage = Server::sync_service_result(&graph, Some(&result), &storage_arg).unwrap();
		assert_eq!(storage.is_err(), failure);
		if !failure {
			assert!(matches!(storage, Ok(None)));
		}
	}
}

#[test]
fn graph_notifies_ancestors_and_inherited_children_without_retaining_control() {
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
	graph.set_service(sender.downgrade());
	let parent: tg::object::Id = tg::blob::Id::new(b"parent").into();
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
		storage: Some(tg::object::storage::Set::NODE),
	};
	graph.update_object_local(update);
	while receiver.try_recv().is_ok() {}
	let child: tg::object::Id = child.into();
	let output = protocol::Output::Object(protocol::ObjectOutput {
		permissions: tg::authorization::permission::object::Set::SUBTREE,
		storage: tg::object::storage::Set::NODE | tg::object::storage::Set::SUBTREE,
		tokens: Vec::new(),
	});
	graph
		.update_node_local_service_output(&child.clone().into(), &output)
		.unwrap();
	let Event::Nodes(nodes) = receiver.try_recv().unwrap() else {
		panic!("expected node notifications")
	};
	assert!(nodes.contains(&parent.clone().into()));
	assert!(nodes.contains(&child.clone().into()));
	let permissions = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::SUBTREE,
	);
	assert!(
		graph
			.object_local_permissions(&parent)
			.contains(permissions)
	);
	let descendant: tg::object::Id = tg::blob::Id::new(b"descendant").into();
	graph.update_object_remote(false, &descendant, Some(parent.into()), None, None);
	let Event::Nodes(nodes) = receiver.try_recv().unwrap() else {
		panic!("expected node notifications")
	};
	assert!(nodes.contains(&descendant.clone().into()));
	assert!(
		graph
			.object_local_permissions(&descendant)
			.contains(permissions)
	);
	drop(sender);
	assert!(receiver.is_closed());
}

fn graph() -> Mutex<Graph> {
	let arg = tg::sync::Arg::default();
	Mutex::new(Graph::new(&arg, false))
}
