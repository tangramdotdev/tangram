use {
	super::{Db, Index, Key, Request},
	crate::Tag,
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_tags(&self, tags: &[String]) -> tg::Result<()> {
		if tags.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteTags(tags.to_vec());
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(super) fn task_delete_tags(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		tags: &[String],
	) -> tg::Result<()> {
		for tag in tags {
			let key = Key::Tag(tag.clone()).pack_to_vec();
			let value = db
				.get(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to get the tag"))?;
			let bytes = value.ok_or_else(|| tg::error!("tag not found"))?;
			let tag_data = Tag::deserialize(bytes)?;
			let item = match &tag_data.item {
				tg::Either::Left(id) => id.to_bytes().to_vec(),
				tg::Either::Right(id) => id.to_bytes().to_vec(),
			};
			let item_tag_key = Key::ItemTag {
				item,
				tag: tag.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &item_tag_key)
				.map_err(|source| tg::error!(!source, "failed to delete the item tag"))?;
			match &tag_data.item {
				tg::Either::Left(id) => {
					Self::decrement_object_reference_count(db, transaction, id)?;
				},
				tg::Either::Right(id) => {
					Self::decrement_process_reference_count(db, transaction, id)?;
				},
			}
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete the tag"))?;
		}
		Ok(())
	}
}
