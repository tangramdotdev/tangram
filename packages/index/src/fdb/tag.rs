use {
	super::{Index, Key, Kind, TagCoreField, TagField},
	crate::PutTagArg,
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_tags(&self, args: &[PutTagArg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		self.database
			.run(|txn, _| {
				let this = self.clone();
				let tags = args.to_vec();
				async move {
					this.put_tags_inner(&txn, &tags)
						.await
						.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to put tags"))
	}

	async fn put_tags_inner(&self, txn: &fdb::Transaction, args: &[PutTagArg]) -> tg::Result<()> {
		for arg in args {
			self.put_tag(txn, arg).await?;
		}
		Ok(())
	}

	async fn put_tag(&self, txn: &fdb::Transaction, arg: &PutTagArg) -> tg::Result<()> {
		let key = Key::Tag {
			tag: arg.tag.clone(),
			field: TagField::Core(TagCoreField::Item),
		};
		let key = self.pack(&key);
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the tag"))?
			.map(|item_bytes| {
				if let Ok(id) = tg::object::Id::from_slice(&item_bytes) {
					Ok(tg::Either::Left(id))
				} else if let Ok(id) = tg::process::Id::from_slice(&item_bytes) {
					Ok(tg::Either::Right(id))
				} else {
					Err(tg::error!("failed to parse the item"))
				}
			})
			.transpose()?;
		if let Some(item) = existing
			&& item != arg.item
		{
			let item_bytes = match &item {
				tg::Either::Left(id) => id.to_bytes().to_vec(),
				tg::Either::Right(id) => id.to_bytes().to_vec(),
			};
			let key = Key::ItemTag {
				item: item_bytes,
				tag: arg.tag.clone(),
			};
			let key = self.pack(&key);
			txn.clear(&key);

			match &item {
				tg::Either::Left(id) => {
					self.decrement_object_reference_count(txn, id).await?;
				},
				tg::Either::Right(id) => {
					self.decrement_process_reference_count(txn, id).await?;
				},
			}
		}

		let key = Key::Tag {
			tag: arg.tag.clone(),
			field: TagField::Core(TagCoreField::Item),
		};
		let key = self.pack(&key);
		let item = match &arg.item {
			tg::Either::Left(id) => id.to_bytes().to_vec(),
			tg::Either::Right(id) => id.to_bytes().to_vec(),
		};
		txn.set(&key, &item);

		let key = self.pack(&Key::ItemTag {
			item,
			tag: arg.tag.clone(),
		});
		txn.set(&key, &[]);

		Ok(())
	}

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
		for tag in tags {
			self.delete_tag(txn, tag).await?;
		}
		Ok(())
	}

	async fn delete_tag(&self, txn: &fdb::Transaction, tag: &str) -> tg::Result<()> {
		let key = self.pack(&Key::Tag {
			tag: tag.to_owned(),
			field: TagField::Core(TagCoreField::Item),
		});
		let item = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get tag"))?;
		// Skip tags that do not exist in the index. This can happen for "branch tags" which
		// exist in the database as parent nodes but have no associated item.
		let Some(item) = item else {
			return Ok(());
		};
		let key = self.pack(&Key::ItemTag {
			item: item.to_vec(),
			tag: tag.to_owned(),
		});
		txn.clear(&key);

		if let Ok(id) = tg::object::Id::from_slice(&item) {
			self.decrement_object_reference_count(txn, &id).await?;
		} else if let Ok(id) = tg::process::Id::from_slice(&item) {
			self.decrement_process_reference_count(txn, &id).await?;
		} else {
			return Err(tg::error!("failed to parse tag item"));
		}

		let prefix = (Kind::Tag.to_i32().unwrap(), tag);
		let prefix = self.pack(&prefix);
		let subspace = Subspace::from_bytes(prefix);
		let (begin, end) = subspace.range();
		txn.clear_range(&begin, &end);

		Ok(())
	}
}
