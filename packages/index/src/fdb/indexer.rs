#![allow(clippy::unnecessary_wraps)]

use {
	crate::fdb::{Index, Key as IndexKey, Kind, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::{StreamExt as _, pin_mut},
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
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

	pub(crate) async fn delete_indexer_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		arg: &crate::indexer::delete::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = IndexKey::Indexer(Key::Indexer(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		txn.clear(&key);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn get_indexers_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
	) -> tg::Result<ControlFlow<Vec<crate::indexer::Indexer>, fdb::FdbError>> {
		let prefix = Self::pack(subspace, &(Kind::Indexer.to_i32().unwrap(),));
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		let entries = txn.get_ranges_keyvalues(range, false);
		pin_mut!(entries);
		let mut indexers = Vec::new();
		while let Some(result) = entries.next().await {
			let entry = crate::fdb::retry!(result);
			let key = Self::unpack(subspace, entry.key())?;
			let IndexKey::Indexer(Key::Indexer(id)) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			let indexer = crate::indexer::Indexer::deserialize(id, entry.value())?;
			indexers.push(indexer);
		}

		Ok(ControlFlow::Break(indexers))
	}

	pub(crate) async fn put_indexer_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		arg: &crate::indexer::put::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = IndexKey::Indexer(Key::Indexer(arg.indexer.id.clone()));
		let key = Self::pack(subspace, &key);
		let value = arg.indexer.serialize()?;
		txn.set(&key, &value);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn try_get_indexer_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		arg: &crate::indexer::get::Arg,
	) -> tg::Result<ControlFlow<Option<crate::indexer::Indexer>, fdb::FdbError>> {
		let key = IndexKey::Indexer(Key::Indexer(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let value = crate::fdb::retry!(result);
		let indexer = value
			.map(|value| crate::indexer::Indexer::deserialize(arg.id.clone(), &value))
			.transpose()?;

		Ok(ControlFlow::Break(indexer))
	}

	pub(crate) async fn update_indexer_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		arg: &crate::indexer::update::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = IndexKey::Indexer(Key::Indexer(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let value = crate::fdb::retry!(result)
			.ok_or_else(|| tg::error!(id = %arg.id, "the indexer does not exist"))?;
		let mut indexer = crate::indexer::Indexer::deserialize(arg.id.clone(), &value)?;
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
		txn.set(&key, &value);

		Ok(ControlFlow::Break(()))
	}
}
