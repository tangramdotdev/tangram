use {tangram_client::prelude::*, tangram_index::permission::capture::enqueue};

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn permission_capture_queue_coalesces_and_preserves_new_attempts() {
	let options = crate::Options {
		cleaning_partition_total: 1,
		cluster: std::env::var_os("FDB_CLUSTER_FILE")
			.expect("set FDB_CLUSTER_FILE")
			.into(),
		instance: Some(format!(
			"index_capture_test_{:032x}/",
			rand::random::<u128>()
		)),
		max_process_depth: None,
		object_permission_time_to_touch: std::time::Duration::ZERO,
		max_write_operation_batch_size: 1024,
		permission_update_partition_total: 1,
		process_data_fragment_size: 16,
		process_permission_time_to_touch: std::time::Duration::ZERO,
		read_channel_capacity: 256,
		read_request_batch_size: 4,
		read_task_concurrency: 16,
		read_task_count: 4,
		storage_and_metadata_update_partition_total: 1,
		storage_relation_batch_size: 8,
		usage_partition_total: 1,
		usage_update_partition_total: 1,
		verification: crate::VerificationConfig { concurrency: 64 },
		write_operation_batch_size: 1024,
		write_transaction_concurrency: 1,
	};
	super::run(&options, async |index| {
		let resource = tg::object::Id::from(tg::file::Id::new(b"queued")).into();
		let arg = enqueue::Arg { resource };
		index.enqueue_permission_capture(arg.clone()).await.unwrap();
		let first = index
			.permission_capture_batch(1, 0, 1)
			.await
			.unwrap()
			.pop()
			.unwrap();
		index.enqueue_permission_capture(arg.clone()).await.unwrap();
		index.complete_permission_capture(&first).await.unwrap();
		let second = index.permission_capture_batch(8, 0, 1).await.unwrap();
		assert_eq!(second.len(), 1);
		assert_ne!(second[0].attempt, first.attempt);
		index.complete_permission_capture(&second[0]).await.unwrap();
		assert!(
			index
				.permission_capture_batch(8, 0, 1)
				.await
				.unwrap()
				.is_empty()
		);
		// A stale completion must also leave an attempt enqueued after deletion intact.
		index.enqueue_permission_capture(arg).await.unwrap();
		index.complete_permission_capture(&second[0]).await.unwrap();
		assert_eq!(
			index.permission_capture_batch(8, 0, 1).await.unwrap().len(),
			1
		);
		assert!(
			index
				.permission_capture_batch(0, 0, 1)
				.await
				.unwrap()
				.is_empty()
		);
		assert!(
			index
				.permission_capture_batch(8, 1, 2)
				.await
				.unwrap()
				.is_empty()
		);
		// Keep multiple capture sources, then retire every context when permissions become terminal.
		let object = tg::object::Id::from(tg::file::Id::new(b"captured"));
		let tag = tg::tag::Id::new();
		let mut tag_arg = tangram_index::tag::put::Arg {
			account: None,
			id: tag.clone(),
			name: "capture".into(),
			parent: None,
			specifier: "capture".parse().unwrap(),
			target: tg::Either::Left(object.clone()),
			touched_at: 0,
			version: "first".into(),
		};
		index
			.put_tags(std::slice::from_ref(&tag_arg))
			.await
			.unwrap();
		let capture = tangram_index::permission::capture::Capture {
			expires_at: i64::MAX,
			principal: tg::Principal::Root,
			resource: tg::Referent::with_node(object.clone().into()),
			sources: std::collections::BTreeSet::new(),
			subject: tag.clone().into(),
			version: Some("first".into()),
		};
		let mut other = capture.clone();
		other.principal = tg::Principal::Anonymous;
		let batch = tangram_index::batch::Arg {
			items: vec![
				tangram_index::batch::Item::PutPermissionCapture(capture.clone()),
				tangram_index::batch::Item::PutPermissionCapture(other),
			],
		};
		index.batch(batch).await.unwrap().unwrap();
		assert_eq!(
			index
				.get_permission_captures(&object.clone().into())
				.await
				.unwrap()
				.len(),
			2
		);
		let permission = tangram_index::permission::put::Arg {
			created_at: 0,
			creator: None,
			permissions: tg::authorization::permission::Set::Object(
				tg::authorization::permission::object::Set::SUBTREE,
			),
			resource: object.clone().into(),
			source: tangram_index::permission::Source::Direct { expires_at: None },
			subject: tg::authorization::Subject::Tag(tag.clone()),
			time_to_touch: None,
			version: Some("first".into()),
		};
		index.put_permissions(&[permission]).await.unwrap();
		let batch = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutPermissionCapture(
				capture.clone(),
			)],
		};
		index.batch(batch).await.unwrap().unwrap();
		assert_eq!(
			index
				.get_permission_captures(&object.clone().into())
				.await
				.unwrap(),
			[]
		);

		// A queued capture from the old version cannot restore records after retargeting.
		tag_arg.version = "second".into();
		index
			.put_tags(std::slice::from_ref(&tag_arg))
			.await
			.unwrap();
		let mut current = capture.clone();
		current.version = Some("second".into());
		let batch = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutPermissionCapture(current)],
		};
		index.batch(batch).await.unwrap().unwrap();
		assert_eq!(
			index
				.get_permission_captures(&object.clone().into())
				.await
				.unwrap()
				.len(),
			1
		);
		index.delete_tags(&[tag]).await.unwrap();
		let batch = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutPermissionCapture(capture)],
		};
		index.batch(batch).await.unwrap().unwrap();
		assert_eq!(
			index.get_permission_captures(&object.into()).await.unwrap(),
			[]
		);
	})
	.await;
}
