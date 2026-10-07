use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_tags_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::tag::put::Arg],
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for arg in args {
			crate::propagate!(Self::put_tag(txn, subspace, arg, partition_totals).await);
		}
		Ok(ControlFlow::Break(()))
	}

	async fn put_tag(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::tag::put::Arg,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let partition_total = partition_totals.cleaning;
		let key = Key::Tag(crate::tag::Key::Tag(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let tag = crate::retry!(result)
			.map(|bytes| tangram_index::tag::Tag::deserialize(&bytes))
			.transpose()?;
		if let Some(tag) = &tag {
			if tag.version == arg.version && tag.target != arg.target {
				return Err(tg::error!("the tag target changed without a new version"));
			}
			if tag.version != arg.version {
				let subject = tg::authorization::Subject::Tag(arg.id.clone());
				crate::propagate!(
					Self::delete_subject_permissions_with_transaction(
						txn,
						subspace,
						&subject,
						partition_totals
					)
					.await
				);
				crate::propagate!(
					Self::delete_delegations_for_subject_with_transaction(
						txn,
						subspace,
						&subject,
						partition_totals.permission_update
					)
					.await
				);
			}
		}
		if let Some(tag) = tag.as_ref()
			&& (tag.account != arg.account
				|| tag.specifier != arg.specifier
				|| tag.target != arg.target)
		{
			match &tag.target {
				tg::Either::Left(id) => {
					crate::propagate!(
						Self::schedule_object_accounts_for_cleaning(
							txn,
							subspace,
							id,
							partition_totals.usage_update,
						)
						.await
					);
				},
				tg::Either::Right(id) => {
					crate::propagate!(
						Self::schedule_process_accounts_for_cleaning(
							txn,
							subspace,
							id,
							partition_totals.usage_update,
						)
						.await
					);
				},
			}
		}
		if let Some(tag) = tag.as_ref()
			&& tag.target != arg.target
		{
			let target = match &tag.target {
				tg::Either::Left(id) => id.to_bytes().to_vec(),
				tg::Either::Right(id) => id.to_bytes().to_vec(),
			};
			let old_key = Key::Tag(crate::tag::Key::TargetTag {
				target,
				tag: arg.id.clone(),
			});
			let old_key = Self::pack(subspace, &old_key);
			txn.clear(&old_key);

			match &tag.target {
				tg::Either::Left(id) => {
					crate::propagate!(
						Self::decrement_object_reference_count(txn, subspace, id, partition_total)
							.await
					);
				},
				tg::Either::Right(id) => {
					crate::propagate!(
						Self::decrement_process_reference_count(txn, subspace, id, partition_total)
							.await
					);
				},
			}
		}
		if let Some(tag) = tag.as_ref()
			&& (tag.parent != arg.parent || tag.name != arg.name)
		{
			let parent_tag_key = Key::Tag(crate::tag::Key::ParentTag {
				parent: tag.parent.clone(),
				name: tag.name.clone(),
				tag: arg.id.clone(),
			});
			let parent_tag_key = Self::pack(subspace, &parent_tag_key);
			txn.clear(&parent_tag_key);

			let tag_parent_key = Key::Tag(crate::tag::Key::TagParent {
				tag: arg.id.clone(),
				parent: tag.parent.clone(),
				name: tag.name.clone(),
			});
			let tag_parent_key = Self::pack(subspace, &tag_parent_key);
			txn.clear(&tag_parent_key);
		}
		if let Some(tag) = tag.as_ref()
			&& tag.specifier != arg.specifier
		{
			let node_key = Key::Node(crate::node::Key::Node(tag.specifier.clone()));
			let node_key = Self::pack(subspace, &node_key);
			txn.clear(&node_key);
		}

		let key = Key::Tag(crate::tag::Key::Tag(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let value = tangram_index::tag::Tag {
			account: arg.account.clone(),
			name: arg.name.clone(),
			parent: arg.parent.clone(),
			specifier: arg.specifier.clone(),
			target: arg.target.clone(),
			version: arg.version.clone(),
		}
		.serialize()?;
		txn.set(&key, &value);

		let node_key = Key::Node(crate::node::Key::Node(arg.specifier.clone()));
		let node_key = Self::pack(subspace, &node_key);
		let node_value = tg::Id::from(arg.id.clone()).to_bytes();
		txn.set(&node_key, node_value.as_ref());

		let target = match &arg.target {
			tg::Either::Left(id) => id.to_bytes().to_vec(),
			tg::Either::Right(id) => id.to_bytes().to_vec(),
		};
		let target_tag_key = Key::Tag(crate::tag::Key::TargetTag {
			target,
			tag: arg.id.clone(),
		});
		let target_tag_key = Self::pack(subspace, &target_tag_key);
		txn.set(&target_tag_key, &[]);

		let parent_tag_key = Key::Tag(crate::tag::Key::ParentTag {
			parent: arg.parent.clone(),
			name: arg.name.clone(),
			tag: arg.id.clone(),
		});
		let parent_tag_key = Self::pack(subspace, &parent_tag_key);
		txn.set(&parent_tag_key, &[]);

		let tag_parent_key = Key::Tag(crate::tag::Key::TagParent {
			tag: arg.id.clone(),
			parent: arg.parent.clone(),
			name: arg.name.clone(),
		});
		let tag_parent_key = Self::pack(subspace, &tag_parent_key);
		txn.set(&tag_parent_key, &[]);

		if tag.as_ref().is_none_or(|tag| tag.account != arg.account) {
			let resources = crate::propagate!(
				Self::get_tag_storage_resources_with_transaction(txn, subspace, &arg.id).await
			);
			for resource in resources {
				if let Ok(object) = tg::object::Id::try_from(resource.clone()) {
					crate::propagate!(
						Self::schedule_object_accounts_for_cleaning(
							txn,
							subspace,
							&object,
							partition_totals.usage_update
						)
						.await
					);
					crate::propagate!(
						Self::enqueue_account_object_from_parents(
							txn,
							subspace,
							&object,
							partition_totals.usage_update,
							arg.touched_at
						)
						.await
					);
				} else if let Ok(process) = tg::process::Id::try_from(resource) {
					crate::propagate!(
						Self::schedule_process_accounts_for_cleaning(
							txn,
							subspace,
							&process,
							partition_totals.usage_update
						)
						.await
					);
					crate::propagate!(
						Self::enqueue_account_process_from_parents(
							txn,
							subspace,
							&process,
							partition_totals.usage_update,
							arg.touched_at
						)
						.await
					);
				}
			}
		}

		Ok(ControlFlow::Break(()))
	}
}
