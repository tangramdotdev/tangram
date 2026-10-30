use {
	super::super::Index, std::collections::BTreeSet, tangram_client::prelude::*,
	tangram_index::prelude::*,
};

#[tokio::test]
async fn incomplete_parents_complete_after_their_descendants_are_put() {
	let (_dir, index) = super::new_index();
	let leaf = object_arg(0, [], 1);
	let middle = object_arg(1, [leaf.id.clone()], 2);
	let top = object_arg(2, [middle.id.clone()], 4);

	// Put the parents before their descendants.
	write(&index, vec![tangram_index::Item::PutObject(top.clone())]).await;
	drain(&index).await;
	write(&index, vec![tangram_index::Item::PutObject(middle.clone())]).await;
	drain(&index).await;
	let objects = index
		.try_get_objects(&[top.id.clone(), middle.id.clone()])
		.await
		.unwrap();
	for object in objects {
		let object = object.unwrap();
		assert!(!object.storage.contains(tg::object::storage::Set::SUBTREE));
		assert_eq!(object.metadata.subtree.count, None);
	}

	// Put the leaf.
	write(&index, vec![tangram_index::Item::PutObject(leaf)]).await;
	drain(&index).await;

	// Verify that the parents are complete.
	let top = index.try_get_object(&top.id).await.unwrap().unwrap();
	assert!(top.complete());
	assert!(top.storage.contains(tg::object::storage::Set::SUBTREE));
	assert_eq!(top.metadata.subtree.count, Some(3));
	assert_eq!(top.metadata.subtree.depth, Some(3));
	assert_eq!(top.metadata.subtree.size, Some(7));
	let middle = index.try_get_object(&middle.id).await.unwrap().unwrap();
	assert!(middle.complete());
	assert_eq!(middle.metadata.subtree.count, Some(2));
}

#[tokio::test]
async fn incomplete_parents_complete_after_their_children_are_already_complete() {
	let (_dir, index) = super::new_index();
	let leaf = object_arg(0, [], 1);
	let parent = object_arg(1, [leaf.id.clone()], 2);

	// Put the child and complete it.
	write(&index, vec![tangram_index::Item::PutObject(leaf.clone())]).await;
	drain(&index).await;
	let leaf = index.try_get_object(&leaf.id).await.unwrap().unwrap();
	assert!(leaf.complete());

	// Put the parent.
	write(&index, vec![tangram_index::Item::PutObject(parent.clone())]).await;
	drain(&index).await;

	// Verify that the parent is complete.
	let parent = index.try_get_object(&parent.id).await.unwrap().unwrap();
	assert!(parent.complete());
	assert!(parent.storage.contains(tg::object::storage::Set::SUBTREE));
	assert_eq!(parent.metadata.subtree.count, Some(2));
	assert_eq!(parent.metadata.subtree.size, Some(3));
}

#[tokio::test]
async fn incomplete_parents_derive_the_metadata_of_children_whose_storage_never_completes() {
	let (_dir, index) = super::new_index();
	let leaf = object_arg(0, [], 1);
	let middle = object_arg(1, [leaf.id.clone()], 2);
	let top = object_arg(2, [middle.id.clone()], 4);

	// Put the parents before the leaf.
	write(
		&index,
		vec![
			tangram_index::Item::PutObject(top.clone()),
			tangram_index::Item::PutObject(middle.clone()),
		],
	)
	.await;
	drain(&index).await;

	// Put the leaf without storage, so that the parents never have subtree storage.
	let mut leaf = leaf;
	leaf.storage = tg::object::storage::Set::empty();
	write(&index, vec![tangram_index::Item::PutObject(leaf)]).await;
	drain(&index).await;

	// Verify that the top derived its subtree metadata.
	let top = index.try_get_object(&top.id).await.unwrap().unwrap();
	assert!(!top.storage.contains(tg::object::storage::Set::SUBTREE));
	assert_eq!(top.metadata.subtree.count, Some(3));
	assert_eq!(top.metadata.subtree.size, Some(7));
}

#[tokio::test]
async fn incomplete_processes_complete_after_their_children_and_objects_are_put() {
	let (_dir, index) = super::new_index();
	let parent_command = object_arg(10, [], 1);
	let child_command = object_arg(11, [], 2);
	let child = process_arg(tg::process::Id::new(), Vec::new(), &child_command.id);
	let parent = process_arg(
		tg::process::Id::new(),
		vec![child.id.clone()],
		&parent_command.id,
	);

	// Put the parent before its child and its command.
	write(
		&index,
		vec![tangram_index::Item::PutProcess(parent.clone())],
	)
	.await;
	drain(&index).await;
	let process = index.try_get_process(&parent.id).await.unwrap().unwrap();
	assert!(!process.complete());
	assert_eq!(process.metadata.subtree.count, None);

	// Put the commands and the child.
	write(
		&index,
		vec![
			tangram_index::Item::PutObject(parent_command),
			tangram_index::Item::PutObject(child_command),
			tangram_index::Item::PutProcess(child),
		],
	)
	.await;
	drain(&index).await;

	// Verify that the parent is complete.
	let process = index.try_get_process(&parent.id).await.unwrap().unwrap();
	assert!(process.complete());
	assert!(process.storage.contains(
		tg::process::storage::Set::SUBTREE | tg::process::storage::Set::SUBTREE_COMMAND_OBJECTS
	));
	assert_eq!(process.metadata.subtree.count, Some(2));
	assert_eq!(process.metadata.subtree.depth, Some(2));
	assert_eq!(process.metadata.node.command_objects.size, Some(1));
	assert_eq!(process.metadata.subtree.command_objects.size, Some(3));
}

fn object_arg(
	value: u64,
	children: impl IntoIterator<Item = tg::object::Id>,
	size: u64,
) -> tangram_index::object::put::Arg {
	let id = tg::object::Id::new(tg::object::Kind::Blob, &value.to_le_bytes().to_vec().into());
	let children = children.into_iter().collect::<BTreeSet<_>>();
	let metadata = tg::object::Metadata {
		node: tg::object::metadata::Node {
			size,
			..Default::default()
		},
		..Default::default()
	};
	tangram_index::object::put::Arg {
		account: None,
		checkout: None,
		children,
		id,
		metadata,
		put: [1; 16],
		storage: tg::object::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 1,
	}
}

fn process_arg(
	id: tg::process::Id,
	children: Vec<tg::process::Id>,
	command: &tg::object::Id,
) -> tangram_index::process::put::Arg {
	let children = children
		.into_iter()
		.map(|child| tg::process::data::Child {
			cached: false,
			process: tg::Referent::with_node(child),
		})
		.collect();
	tangram_index::process::put::Arg {
		account: None,
		cached: false,
		captures: tangram_index::permission::capture::Captures::default(),
		children: Some(children),
		command: Some(vec![command.clone()]),
		command_id: command.clone(),
		data: None,
		error: Some(None),
		id: id.clone(),
		location: None,
		log: Some(None),
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: Some(None),
		parent: None,
		permissions: Vec::new(),
		principal: tg::Principal::Process(id),
		sandbox: None,
		storage: tg::process::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 1,
	}
}

async fn write(index: &Index, items: Vec<tangram_index::Item>) {
	let arg = tangram_index::write::Arg { items };
	index.write(arg).await.unwrap().unwrap();
}

async fn drain(index: &Index) {
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::StorageAndMetadata, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
}
