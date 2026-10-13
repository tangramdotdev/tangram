use {super::super::Index, futures::future, std::time::Duration, tangram_client::prelude::*};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn concurrent_verifications_complete_within_the_transaction_lifetime() {
	let options = crate::Options {
		cleaning_partition_total: 1,
		cluster: std::env::var_os("FDB_CLUSTER_FILE")
			.expect("set FDB_CLUSTER_FILE")
			.into(),
		instance: Some(format!(
			"index_reader_test_{:032x}/",
			rand::random::<u128>()
		)),
		max_process_depth: None,
		max_write_operation_batch_size: 1024,
		permission_update_partition_total: 1,
		read_channel_capacity: 256,
		read_request_batch_size: 64,
		read_transaction_concurrency: 64,
		storage_and_metadata_update_partition_total: 1,
		storage_relation_batch_size: 8,
		usage_partition_total: 1,
		usage_update_partition_total: 1,
		verification: crate::VerificationConfig { concurrency: 64 },
		write_operation_batch_size: 1024,
		write_transaction_concurrency: 1,
	};
	super::run(&options, async |index| {
		// Create a tree that a token grants and a resource tree whose subtree is not granted.
		let leaf = super::directory(&[]);
		let (granted, granted_objects) = tree("granted", 32, 64, &leaf.id);
		let (resource, resource_objects) = tree("resource", 32, 32, &leaf.id);
		let objects = [vec![leaf], granted_objects, resource_objects].concat();
		super::put(index, objects).await;
		drain(index).await;

		// Request the subtree permission on the resource while holding only its node permission, so that verification evaluates the resource's subtree.
		let mut set = tg::authorization::permission::object::Set::empty();
		set.insert(tg::authorization::permission::object::Set::NODE);
		set.insert(tg::authorization::permission::object::Set::SUBTREE);
		let permissions = tg::authorization::permission::Set::Object(set);
		let tokens = vec![
			token(
				&granted,
				tg::authorization::permission::object::Permission::Subtree,
			),
			token(
				&resource,
				tg::authorization::permission::object::Permission::Node,
			),
		];
		let arg = tangram_index::verify::Arg {
			requested: permissions,
			required: permissions,
			resource: tg::Selector::Id(resource.into()),
			storage: tg::storage::Set::Object(tg::object::storage::Set::empty()),
			subject: None,
			tokens,
		};
		let args = [arg];
		let config = tangram_index::verify::Config::default();
		let principal = tg::Principal::User(tg::user::Id::new());

		// Verify concurrent copies of the request, whose combined verification work exceeds the transaction lifetime when it runs on one thread.
		let count = 64;
		let deadline = Duration::from_secs(30);
		let outputs = future::join_all((0..count).map(|_| {
			tokio::time::timeout(deadline, index.verify_batch(&args, config, &principal))
		}))
		.await;
		let completed = outputs
			.iter()
			.filter(|output| matches!(output, Ok(Ok(_))))
			.count();
		assert_eq!(
			completed, count,
			"the concurrent verifications did not complete before the deadline"
		);
	})
	.await;
}

fn tree(
	name: &str,
	directories: usize,
	leaves: usize,
	leaf: &tg::object::Id,
) -> (tg::object::Id, Vec<tangram_index::object::put::Arg>) {
	let mut objects = Vec::new();
	let mut children = Vec::new();
	for directory_index in 0..directories {
		let mut entries = Vec::new();
		for leaf_index in 0..leaves {
			let entry = format!("{name}_{directory_index}_{leaf_index}");
			let object = super::directory(&[(&entry, leaf.clone())]);
			entries.push((leaf_index.to_string(), object.id.clone()));
			objects.push(object);
		}
		let entries = entries
			.iter()
			.map(|(name, id)| (name.as_str(), id.clone()))
			.collect::<Vec<_>>();
		let directory = super::directory(&entries);
		children.push((directory_index.to_string(), directory.id.clone()));
		objects.push(directory);
	}
	let children = children
		.iter()
		.map(|(name, id)| (name.as_str(), id.clone()))
		.collect::<Vec<_>>();
	let root = super::directory(&children);
	let id = root.id.clone();
	objects.push(root);

	(id, objects)
}

fn token(
	resource: &tg::object::Id,
	permission: tg::authorization::permission::object::Permission,
) -> tg::authorization::Body {
	let permissions = vec![tg::authorization::Permission::Object(permission)];
	let resource = resource.clone().into();
	tg::authorization::Body {
		expires_at: i64::MAX,
		permissions,
		resource,
	}
}

async fn drain(index: &Index) {
	for _ in 0..100 {
		let output = index
			.update_batch(tangram_index::update::Kind::StorageAndMetadata, 1024, 0, 1)
			.await
			.unwrap();
		if output.count == 0 {
			return;
		}
	}
	panic!("the storage and metadata updates did not drain");
}
