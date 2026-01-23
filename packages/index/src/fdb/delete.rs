use {
	super::{Index, Key, Kind, TagCoreField, TagField},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_tags(&self, tags: &[String]) -> tg::Result<()> {
		if tags.is_empty() {
			return Ok(());
		}
		self.database
			.run(|txn, _| {
				let this = self.clone();
				let tags = tags.to_vec();
				async move {
					this.delete_tags_inner(&txn, &tags)
						.await
						.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to delete tags"))
	}

	async fn delete_tags_inner(&self, txn: &fdb::Transaction, tags: &[String]) -> tg::Result<()> {
		futures::future::try_join_all(tags.iter().map(|tag| self.delete_tag(txn, tag))).await?;
		Ok(())
	}

	async fn delete_tag(&self, txn: &fdb::Transaction, tag: &str) -> tg::Result<()> {
		let key = Key::Tag {
			tag: tag.to_owned(),
			field: TagField::Core(TagCoreField::Item),
		};
		let key = self.pack(&key);
		let item_bytes = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get tag"))?;
		let item_bytes = item_bytes.ok_or_else(|| tg::error!("tag not found"))?;
		let key = Key::ItemTag {
			item: item_bytes.to_vec(),
			tag: tag.to_owned(),
		};
		let key = self.pack(&key);
		txn.clear(&key);
		if let Ok(object_id) = tg::object::Id::from_slice(&item_bytes) {
			self.decrement_object_reference_count(txn, &object_id)
				.await?;
		} else if let Ok(process_id) = tg::process::Id::from_slice(&item_bytes) {
			self.decrement_process_reference_count(txn, &process_id)
				.await?;
		}
		let prefix = (Kind::Tag.to_i32().unwrap(), tag);
		let prefix = self.pack(&prefix);
		let subspace = Subspace::from_bytes(prefix);
		let (begin, end) = subspace.range();
		txn.clear_range(&begin, &end);
		Ok(())
	}
}
