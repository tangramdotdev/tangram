use {
	crate::lmdb::{Db, Index, Key as IndexKey, Kind, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub enum Key {
	Indexer(tg::indexer::Id),
}

impl Index {
	pub async fn delete_indexer(&self, arg: crate::indexer::delete::Arg) -> tg::Result<()> {
		let request = Request::DeleteIndexer(arg);
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub async fn get_indexers(&self) -> tg::Result<Vec<crate::indexer::Indexer>> {
		let response = self
			.send_read_request(crate::read::Request::GetIndexers)
			.await?;
		let crate::read::Response::GetIndexers(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn put_indexer(&self, arg: crate::indexer::put::Arg) -> tg::Result<()> {
		let request = Request::PutIndexer(arg);
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub async fn try_get_indexer(
		&self,
		arg: crate::indexer::get::Arg,
	) -> tg::Result<Option<crate::indexer::Indexer>> {
		let request = crate::read::Request::TryGetIndexer(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetIndexer(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn update_indexer(&self, arg: crate::indexer::update::Arg) -> tg::Result<()> {
		let request = Request::UpdateIndexer(arg);
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub(crate) fn delete_indexer_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::indexer::delete::Arg,
	) -> tg::Result<()> {
		let key = IndexKey::Indexer(Key::Indexer(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		db.delete(transaction, &key)
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to delete the indexer"))?;

		Ok(())
	}

	pub(crate) fn get_indexers_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
	) -> tg::Result<Vec<crate::indexer::Indexer>> {
		let prefix = Self::pack(subspace, &(Kind::Indexer.to_i32().unwrap(),));
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the indexers"))?;
		entries
			.map(|entry| {
				let (key, value) =
					entry.map_err(|error| tg::error!(!error, "failed to get an indexer entry"))?;
				let key = Self::unpack(subspace, key)?;
				let IndexKey::Indexer(Key::Indexer(id)) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				crate::indexer::Indexer::deserialize(id, value)
			})
			.collect()
	}

	pub(crate) fn put_indexer_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::indexer::put::Arg,
	) -> tg::Result<()> {
		let key = IndexKey::Indexer(Key::Indexer(arg.indexer.id.clone()));
		let key = Self::pack(subspace, &key);
		let value = arg.indexer.serialize()?;
		db.put(transaction, &key, &value).map_err(
			|error| tg::error!(!error, id = %arg.indexer.id, "failed to put the indexer"),
		)?;

		Ok(())
	}

	pub(crate) fn try_get_indexer_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		arg: &crate::indexer::get::Arg,
	) -> tg::Result<Option<crate::indexer::Indexer>> {
		let key = IndexKey::Indexer(Key::Indexer(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to get the indexer"))?;
		value
			.map(|value| crate::indexer::Indexer::deserialize(arg.id.clone(), value))
			.transpose()
	}

	pub(crate) fn update_indexer_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::indexer::update::Arg,
	) -> tg::Result<()> {
		let key = IndexKey::Indexer(Key::Indexer(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to get the indexer"))?
			.ok_or_else(|| tg::error!(id = %arg.id, "the indexer does not exist"))?;
		let mut indexer = crate::indexer::Indexer::deserialize(arg.id.clone(), value)?;
		match &arg.value {
			crate::indexer::update::Value::ArchiveReadSequence(value) => {
				indexer.archive_read_sequence = *value;
			},
			crate::indexer::update::Value::ArchiveWriteSequence(value) => {
				indexer.archive_write_sequence = *value;
			},
			crate::indexer::update::Value::Available(value) => indexer.available = *value,
			crate::indexer::update::Value::IndexReadSequence(value) => {
				indexer.index_read_sequence = *value;
			},
			crate::indexer::update::Value::IndexWriteSequence(value) => {
				indexer.index_write_sequence = *value;
			},
		}
		let value = indexer.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to update the indexer"))?;

		Ok(())
	}
}
