use tangram_client::prelude::*;

fn options() -> Option<crate::Options> {
	let cluster = tangram_tests_util::database::cluster()?;
	let options = options_with_cluster(cluster);
	Some(options)
}

fn options_with_cluster(cluster: std::path::PathBuf) -> crate::Options {
	crate::Options {
		cleaning_partition_total: 1,
		cluster,
		instance: Some(format!("index_node_test_{:032x}/", rand::random::<u128>())),
		max_process_depth: None,
		max_write_operation_batch_size: 1024,
		object_permission_time_to_touch: std::time::Duration::ZERO,
		permission_update_partition_total: 1,
		process_data_fragment_size: 16,
		process_permission_time_to_touch: std::time::Duration::ZERO,
		read_channel_capacity: 256,
		read_request_batch_size: 4,
		read_task_concurrency: 4,
		read_task_count: 1,
		storage_and_metadata_update_partition_total: 1,
		storage_relation_batch_size: 8,
		usage_partition_total: 1,
		usage_update_partition_total: 1,
		verification: crate::VerificationConfig { concurrency: 1 },
		write_operation_batch_size: 1024,
		write_transaction_concurrency: 1,
	}
}

#[tokio::test]
async fn deleting_replaced_nodes_preserves_the_replacement() {
	let Some(options) = options() else {
		return;
	};
	super::run(&options, async |index| {
		let group = tg::group::Id::new();
		let organization = tg::organization::Id::new();
		let tag = tg::tag::Id::new();
		let user = tg::user::Id::new();
		let specifier: tg::Specifier = "test".parse().unwrap();
		let nodes = vec![
			(
				tangram_index::Item::PutGroup(tangram_index::group::put::Arg {
					id: group.clone(),
					parent: None,
					specifier: specifier.clone(),
				}),
				tangram_index::Item::DeleteGroup(group),
			),
			(
				tangram_index::Item::PutOrganization(tangram_index::organization::put::Arg {
					billing_ready: None,
					id: organization.clone(),
					specifier: specifier.clone(),
				}),
				tangram_index::Item::DeleteOrganization(organization),
			),
			(
				tangram_index::Item::PutTag(tangram_index::tag::put::Arg {
					account: None,
					id: tag.clone(),
					name: "test".to_owned(),
					parent: None,
					specifier: specifier.clone(),
					target: tg::Either::Left(tg::file::Id::new(b"target").into()),
					touched_at: 0,
					version: "test".to_owned(),
				}),
				tangram_index::Item::DeleteTag(tag),
			),
			(
				tangram_index::Item::PutUser(tangram_index::user::put::Arg {
					billing_ready: None,
					id: user.clone(),
					specifier: specifier.clone(),
				}),
				tangram_index::Item::DeleteUser(user),
			),
		];
		for (put, delete) in nodes {
			let replacement = tg::group::Id::new();
			let batch = tangram_index::write::Arg {
				items: vec![
					put,
					tangram_index::Item::PutGroup(tangram_index::group::put::Arg {
						id: replacement.clone(),
						parent: None,
						specifier: specifier.clone(),
					}),
					delete,
				],
			};
			index.batch(batch).await.unwrap().unwrap();
			let ids = index
				.try_get_ids_for_specifiers(std::slice::from_ref(&specifier))
				.await
				.unwrap();
			assert_eq!(ids, vec![Some(replacement.into())]);
		}
	})
	.await;
}
