use {
	crate::lmdb::{Db, Index, Key, Request},
	foundationdb_tuple as fdbt, heed as lmdb,
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
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(crate) fn task_delete_tags(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::tag::Id],
	) -> tg::Result<()> {
		for id in ids {
			let key = Key::Tag(crate::lmdb::tag::Key::Tag(id.clone()));
			let key = Self::pack(subspace, &key);
			let value = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the tag"))?;
			let Some(bytes) = value else {
				continue;
			};
			let data = crate::tag::Tag::deserialize(bytes)?;
			let item = match &data.item {
				tg::Either::Left(id) => id.to_bytes().to_vec(),
				tg::Either::Right(id) => id.to_bytes().to_vec(),
			};
			let key = Key::Tag(crate::lmdb::tag::Key::ItemTag {
				item,
				tag: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the item tag"))?;
			let key = Key::Tag(crate::lmdb::tag::Key::ParentTag {
				parent: data.parent.clone(),
				name: data.name.clone(),
				tag: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the parent tag"))?;
			let key = Key::Tag(crate::lmdb::tag::Key::TagParent {
				tag: id.clone(),
				parent: data.parent.clone(),
				name: data.name.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the tag parent"))?;
			match &data.item {
				tg::Either::Left(id) => {
					Self::decrement_object_reference_count(db, subspace, transaction, id)?;
				},
				tg::Either::Right(id) => {
					Self::decrement_process_reference_count(db, subspace, transaction, id)?;
				},
			}
			let key = Key::Tag(crate::lmdb::tag::Key::Tag(id.clone()));
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the tag"))?;
		}
		Ok(())
	}
}
