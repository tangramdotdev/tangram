use {
	super::{Db, Key, Kind, Store},
	crate::indexer,
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Store {
	pub async fn delete_indexer(&self, arg: indexer::delete::Arg) -> tg::Result<()> {
		let request = super::request::Request::DeleteIndexer(arg);

		self.send_write_request(request).await
	}

	pub async fn get_indexers(&self) -> tg::Result<Vec<indexer::Indexer>> {
		let response = self
			.send_read_request(crate::read::Request::GetIndexers)
			.await?;
		let crate::read::Response::GetIndexers(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn put_indexer(&self, arg: indexer::put::Arg) -> tg::Result<()> {
		let request = super::request::Request::PutIndexer(arg);

		self.send_write_request(request).await
	}

	pub async fn try_get_indexer(
		&self,
		arg: indexer::get::Arg,
	) -> tg::Result<Option<indexer::Indexer>> {
		let request = crate::read::Request::TryGetIndexer(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetIndexer(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn update_indexer(&self, arg: indexer::update::Arg) -> tg::Result<()> {
		let request = super::request::Request::UpdateIndexer(arg);

		self.send_write_request(request).await
	}

	pub(super) fn delete_indexer_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &indexer::delete::Arg,
	) -> tg::Result<()> {
		let key = Key::Indexer(&arg.id).pack_to_vec();
		db.delete(transaction, &key)
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to delete the indexer"))?;

		Ok(())
	}

	pub(super) fn get_indexers_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
	) -> tg::Result<Vec<indexer::Indexer>> {
		let prefix = fdbt::pack(&(Kind::Indexer.to_i32().unwrap(),));
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the indexers"))?;
		entries
			.map(|entry| {
				let (key, value) =
					entry.map_err(|error| tg::error!(!error, "failed to get an indexer entry"))?;
				let (_, id): (i32, Vec<u8>) = fdbt::unpack(key)
					.map_err(|error| tg::error!(!error, "failed to unpack an indexer key"))?;
				let id = tg::indexer::Id::from_slice(&id)?;

				decode(id, value)
			})
			.collect()
	}

	pub(super) fn put_indexer_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: indexer::put::Arg,
	) -> tg::Result<()> {
		let indexer = arg.indexer;
		let key = Key::Indexer(&indexer.id).pack_to_vec();
		let value = encode(&indexer);
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, id = %indexer.id, "failed to put the indexer"))?;

		Ok(())
	}

	pub(super) fn try_get_indexer_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &indexer::get::Arg,
	) -> tg::Result<Option<indexer::Indexer>> {
		let key = Key::Indexer(&arg.id).pack_to_vec();
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to get the indexer"))?;
		value.map(|value| decode(arg.id.clone(), value)).transpose()
	}

	pub(super) fn update_indexer_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &indexer::update::Arg,
	) -> tg::Result<()> {
		let key = Key::Indexer(&arg.id).pack_to_vec();
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to get the indexer"))?
			.ok_or_else(|| tg::error!(id = %arg.id, "the indexer does not exist"))?;
		let mut indexer = decode(arg.id.clone(), value)?;
		match &arg.value {
			indexer::update::Value::ArchiveReadSequence(value) => {
				indexer.archive_read_sequence = *value;
			},
			indexer::update::Value::ArchiveWriteSequence(value) => {
				indexer.archive_write_sequence = *value;
			},
			indexer::update::Value::Available(value) => indexer.available = *value,
			indexer::update::Value::IndexReadSequence(value) => {
				indexer.index_read_sequence = *value;
			},
			indexer::update::Value::IndexWriteSequence(value) => {
				indexer.index_write_sequence = *value;
			},
		}
		let value = encode(&indexer);
		db.put(transaction, &key, &value).map_err(
			|error| tg::error!(!error, id = %indexer.id, "failed to update the indexer"),
		)?;

		Ok(())
	}
}

fn decode(id: tg::indexer::Id, value: &[u8]) -> tg::Result<indexer::Indexer> {
	let (
		archive_read_sequence,
		archive_write_sequence,
		available,
		index_read_sequence,
		index_write_sequence,
	): (u64, u64, i32, u64, u64) = fdbt::unpack(value)
		.map_err(|error| tg::error!(!error, "failed to unpack an indexer value"))?;
	let available = match available {
		0 => false,
		1 => true,
		_ => return Err(tg::error!(%available, "invalid indexer availability")),
	};
	let indexer = indexer::Indexer {
		archive_read_sequence,
		archive_write_sequence,
		available,
		id,
		index_read_sequence,
		index_write_sequence,
	};

	Ok(indexer)
}

fn encode(indexer: &indexer::Indexer) -> Vec<u8> {
	let available = i32::from(indexer.available);
	fdbt::pack(&(
		indexer.archive_read_sequence,
		indexer.archive_write_sequence,
		available,
		indexer.index_read_sequence,
		indexer.index_write_sequence,
	))
}
