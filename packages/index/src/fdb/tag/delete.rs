use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_tags(&self, ids: &[tg::tag::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let request = Request::DeleteTags(ids.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub(crate) async fn delete_tags_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		ids: &[tg::tag::Id],
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for id in ids {
			crate::fdb::propagate!(Self::delete_tag(txn, subspace, id, partition_total).await);
		}
		Ok(ControlFlow::Break(()))
	}

	async fn delete_tag(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::tag::Id,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Key::Tag(crate::fdb::tag::Key::Tag(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = crate::fdb::retry!(txn.get(&key, false).await);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(()));
		};
		let data = crate::tag::Tag::deserialize(&bytes)?;
		let target = match &data.target {
			tg::Either::Left(id) => id.to_bytes().to_vec(),
			tg::Either::Right(id) => id.to_bytes().to_vec(),
		};
		let target_tag_key = Key::Tag(crate::fdb::tag::Key::TargetTag {
			target,
			tag: id.clone(),
		});
		let target_tag_key = Self::pack(subspace, &target_tag_key);
		txn.clear(&target_tag_key);

		let parent_tag_key = Key::Tag(crate::fdb::tag::Key::ParentTag {
			parent: data.parent.clone(),
			name: data.name.clone(),
			tag: id.clone(),
		});
		let parent_tag_key = Self::pack(subspace, &parent_tag_key);
		txn.clear(&parent_tag_key);

		let tag_parent_key = Key::Tag(crate::fdb::tag::Key::TagParent {
			tag: id.clone(),
			parent: data.parent.clone(),
			name: data.name.clone(),
		});
		let tag_parent_key = Self::pack(subspace, &tag_parent_key);
		txn.clear(&tag_parent_key);

		let node_key = Key::Node(crate::fdb::node::Key::Node(data.specifier.clone()));
		let node_key = Self::pack(subspace, &node_key);
		txn.clear(&node_key);

		match &data.target {
			tg::Either::Left(id) => {
				crate::fdb::propagate!(
					Self::schedule_object_accounts_for_cleaning(
						txn,
						subspace,
						id,
						partition_total,
					)
					.await
				);
				crate::fdb::propagate!(
					Self::decrement_object_reference_count(txn, subspace, id, partition_total)
						.await
				);
			},
			tg::Either::Right(id) => {
				crate::fdb::propagate!(
					Self::schedule_process_accounts_for_cleaning(
						txn,
						subspace,
						id,
						partition_total,
					)
					.await
				);
				crate::fdb::propagate!(
					Self::decrement_process_reference_count(txn, subspace, id, partition_total)
						.await
				);
			},
		}

		txn.clear(&key);

		Ok(ControlFlow::Break(()))
	}
}
