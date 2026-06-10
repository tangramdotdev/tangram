use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_tags(&self, ids: &[tg::tag::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteTags(ids.to_vec());
		self.sender_high
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(())
	}

	pub(crate) async fn task_delete_tags(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		ids: &[tg::tag::Id],
		partition_total: u64,
	) -> tg::Result<()> {
		for id in ids {
			Self::delete_tag(txn, subspace, id, partition_total).await?;
		}
		Ok(())
	}

	async fn delete_tag(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::tag::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Tag(crate::fdb::tag::Key::Tag(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the tag"))?;
		let Some(bytes) = bytes else {
			return Ok(());
		};
		let data = crate::tag::Tag::deserialize(&bytes)?;
		let item = match &data.item {
			tg::Either::Left(id) => id.to_bytes().to_vec(),
			tg::Either::Right(id) => id.to_bytes().to_vec(),
		};
		let item_tag_key = Key::Tag(crate::fdb::tag::Key::ItemTag {
			item,
			tag: id.clone(),
		});
		let item_tag_key = Self::pack(subspace, &item_tag_key);
		txn.clear(&item_tag_key);

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

		match &data.item {
			tg::Either::Left(id) => {
				Self::decrement_object_reference_count(txn, subspace, id, partition_total).await?;
			},
			tg::Either::Right(id) => {
				Self::decrement_process_reference_count(txn, subspace, id, partition_total).await?;
			},
		}

		txn.clear(&key);

		Ok(())
	}
}
