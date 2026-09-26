use {
	super::{Attempt, Event, Request, State},
	crate::{
		Server,
		sync::graph::{Graph, UpdateObjectLocalArg},
	},
	std::{
		collections::{BTreeMap, BTreeSet},
		sync::{Arc, Mutex},
	},
	tangram_client::{prelude::*, sync::control as protocol},
	tokio::time::Instant,
};

#[test]
fn empty_requests_are_notified_when_a_child_enters_the_graph() {
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
	graph.set_control(sender.downgrade());
	let parent: tg::object::Id = tg::blob::Id::new(b"parent").into();
	let child = tg::blob::Id::new(b"child");
	let request = protocol::GetClientRequestArg {
		node: child.clone().into(),
		permissions: tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::empty(),
		),
		storage: None,
	};
	assert!(
		graph
			.try_get_node_local_control_output(&request)
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
		.try_get_node_local_control_output(&request)
		.unwrap()
		.unwrap();
	assert!(output.permissions().is_empty());
	let protocol::GetServerResponseOutput::Object(output) = output else {
		panic!("expected an object response")
	};
	assert!(output.storage.is_none());
}

#[test]
fn requests_wait_for_their_own_requirements() {
	let id: tg::object::Id = tg::blob::Id::new(b"requirements").into();
	let mut state = state();
	let permissions = tg::authorization::permission::object::Set::SUBTREE;
	request(
		&mut state,
		"permissions",
		protocol::ClientRequestArg::object(id.clone(), permissions, None),
	);
	request(
		&mut state,
		"storage",
		protocol::ClientRequestArg::object(
			id.clone(),
			permissions,
			Some(tg::object::Storage { subtree: true }),
		),
	);
	assert!(!Server::sync_control_create_response(
		&mut state,
		"attempt",
		"permissions"
	));
	assert!(!Server::sync_control_create_response(
		&mut state, "attempt", "storage"
	));
	state.graph.lock().unwrap().update_object_local_permissions(
		&id,
		tg::authorization::permission::Set::Object(permissions),
	);
	assert!(Server::sync_control_create_response(
		&mut state,
		"attempt",
		"permissions"
	));
	assert!(!Server::sync_control_create_response(
		&mut state, "attempt", "storage"
	));
	let response = state.attempts["attempt"].requests["permissions"]
		.response
		.as_ref()
		.unwrap();
	assert!(
		matches!(&response.output, Some(protocol::ServerResponseOutput::Get(Some(protocol::GetServerResponseOutput::Object(output)))) if output.storage.is_none())
	);
	let update = UpdateObjectLocalArg {
		data: None,
		id: &id,
		marked: None,
		metadata: None,
		permissions: None,
		put: None,
		requested: None,
		storage: Some(tg::object::Storage { subtree: true }),
	};
	state.graph.lock().unwrap().update_object_local(update);
	assert!(Server::sync_control_create_response(
		&mut state, "attempt", "storage"
	));
}

#[test]
fn finished_sync_distinguishes_missing_from_permission_only_success() {
	let id: tg::object::Id = tg::blob::Id::new(b"finished").into();
	for failure in [false, true] {
		let mut state = state();
		let permissions = tg::authorization::permission::object::Set::NODE;
		request(
			&mut state,
			"permissions",
			protocol::ClientRequestArg::object(id.clone(), permissions, None),
		);
		request(
			&mut state,
			"storage",
			protocol::ClientRequestArg::object(
				id.clone(),
				permissions,
				Some(tg::object::Storage::default()),
			),
		);
		state.graph.lock().unwrap().update_object_local_permissions(
			&id,
			tg::authorization::permission::Set::Object(permissions),
		);
		let result = if failure {
			Err(tg::error!("the transfer failed"))
		} else {
			Ok(())
		};
		state.finished = Some((Instant::now(), result));
		assert!(Server::sync_control_create_response(
			&mut state,
			"attempt",
			"permissions"
		));
		assert!(Server::sync_control_create_response(
			&mut state, "attempt", "storage"
		));
		let permissions = state.attempts["attempt"].requests["permissions"]
			.response
			.as_ref()
			.unwrap();
		assert!(permissions.error.is_none());
		assert!(matches!(
			&permissions.output,
			Some(protocol::ServerResponseOutput::Get(Some(_)))
		));
		let storage = state.attempts["attempt"].requests["storage"]
			.response
			.as_ref()
			.unwrap();
		assert_eq!(storage.error.is_some(), failure);
		if !failure {
			assert!(matches!(
				&storage.output,
				Some(protocol::ServerResponseOutput::Get(None))
			));
		}
	}
}

#[test]
fn graph_notifies_ancestors_and_inherited_children_without_retaining_control() {
	let arg = tg::sync::Arg::default();
	let mut graph = Graph::new(&arg, false);
	let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
	graph.set_control(sender.downgrade());
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
		storage: Some(tg::object::Storage::default()),
	};
	graph.update_object_local(update);
	while receiver.try_recv().is_ok() {}
	let child: tg::object::Id = child.into();
	let output =
		protocol::GetServerResponseOutput::Object(protocol::GetObjectServerResponseOutput {
			permissions: tg::authorization::permission::object::Set::SUBTREE,
			storage: Some(tg::object::Storage { subtree: true }),
		});
	graph
		.update_node_local_control_output(&child.clone().into(), &output)
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

fn state() -> State {
	let arg = tg::sync::Arg::default();
	State {
		attempts: BTreeMap::new(),
		clients: BTreeMap::new(),
		finished: None,
		graph: Arc::new(Mutex::new(Graph::new(&arg, false))),
		nodes: BTreeMap::new(),
		response_cursor: None,
	}
}

fn request(state: &mut State, id: &str, arg: protocol::ClientRequestArg) {
	let attempt = state
		.attempts
		.entry("attempt".to_owned())
		.or_insert_with(|| Attempt {
			cancelled: BTreeSet::new(),
			client: "client".to_owned(),
			expires_at: Instant::now(),
			requests: BTreeMap::new(),
		});
	let request = Request {
		acknowledged_at: None,
		arg,
		response: None,
	};
	attempt.requests.insert(id.to_owned(), request);
}
