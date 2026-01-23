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
			// First, get the tag value to find the item.
			let key = Key::Tag(tag.clone()).pack_to_vec();
			let value = db
				.get(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to get the tag"))?;

			if let Some(bytes) = value {
				// Parse the tag to get the item.
				if let Ok(tag_data) = Tag::deserialize(bytes) {
					// Delete the item tag relationship.
					let item_bytes: Vec<u8> = match &tag_data.item {
						tg::Either::Left(object_id) => object_id.to_bytes().to_vec(),
						tg::Either::Right(process_id) => process_id.to_bytes().to_vec(),
					};
					let item_tag_key = Key::ItemTag {
						item: item_bytes,
						tag: tag.clone(),
					}
					.pack_to_vec();
					db.delete(transaction, &item_tag_key)
						.map_err(|source| tg::error!(!source, "failed to delete the item tag"))?;
				}
			}

			// Delete the tag itself.
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete the tag"))?;
		}
		Ok(())
	}
}
