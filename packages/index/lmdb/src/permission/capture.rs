use {
	crate::{Db, Index, Key, Kind, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	std::ops::Bound,
	tangram_client::prelude::*,
	tangram_index::permission::capture::{Entry, enqueue},
};

impl Index {
	pub async fn permission_capture_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<Entry>> {
		let request = tangram_index::read::Request::PermissionCaptureBatch {
			batch_size,
			partition_end,
			partition_start,
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::PermissionCaptureBatch(entries) = response else {
			return Err(tg::error!("unexpected permission capture batch response"));
		};
		Ok(entries)
	}

	pub async fn enqueue_permission_capture(&self, arg: enqueue::Arg) -> tg::Result<()> {
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::EnqueuePermissionCapture(arg)],
		};
		self.batch(arg).await?;
		Ok(())
	}

	pub async fn complete_permission_capture(&self, entry: &Entry) -> tg::Result<()> {
		let request = Request::CompletePermissionCapture(entry.clone());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!(
				"unexpected permission capture completion response"
			));
		};
		Ok(())
	}

	pub(crate) fn enqueue_permission_capture_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &enqueue::Arg,
	) -> tg::Result<()> {
		arg.validate()?;
		if !Self::permission_capture_resource_current(db, subspace, transaction, arg)? {
			return Ok(());
		}
		let key = Self::pack(
			subspace,
			&Key::PermissionCapture {
				id: arg.id.clone(),
				partition: 0,
			},
		);
		let bytes = tangram_serialize::to_vec(arg).map_err(|error| {
			tg::error!(!error, "failed to encode a permission capture argument")
		})?;
		db.put(transaction, &key, &bytes).map_err(|error| {
			tg::error!(!error, "failed to enqueue a permission capture argument")
		})?;
		Ok(())
	}

	pub(crate) fn permission_capture_batch_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<Entry>> {
		if batch_size == 0 || partition_start >= partition_end {
			return Ok(Vec::new());
		}
		let begin = subspace.pack(&(Kind::PermissionCapture.to_i32().unwrap(), partition_start));
		let end = subspace.pack(&(Kind::PermissionCapture.to_i32().unwrap(), partition_end));
		let range = (
			Bound::Included(begin.as_slice()),
			Bound::Excluded(end.as_slice()),
		);
		let entries = db
			.range(transaction, &range)
			.map_err(|error| tg::error!(!error, "failed to read permission capture entrys"))?
			.take(batch_size)
			.map(|entry| {
				let (key, bytes) = entry.map_err(|error| {
					tg::error!(!error, "failed to read a permission capture argument")
				})?;
				let Key::PermissionCapture { partition, .. } = Self::unpack(subspace, key)
					.map_err(|error| {
						tg::error!(!error, "failed to decode a permission capture queue key")
					})?
				else {
					return Err(tg::error!("unexpected permission capture key"));
				};
				let arg = tangram_serialize::from_slice(bytes).map_err(|error| {
					tg::error!(!error, "failed to decode a permission capture argument")
				})?;
				Ok(Entry { arg, partition })
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(entries)
	}

	pub(crate) fn complete_permission_capture_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		entry: &Entry,
	) -> tg::Result<()> {
		let key = Self::pack(
			subspace,
			&Key::PermissionCapture {
				id: entry.arg.id.clone(),
				partition: entry.partition,
			},
		);
		db.delete(transaction, &key).map_err(|error| {
			tg::error!(!error, "failed to delete a permission capture argument")
		})?;
		Ok(())
	}

	fn permission_capture_resource_current(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		arg: &enqueue::Arg,
	) -> tg::Result<bool> {
		let Some(version) = &arg.version else {
			return Ok(true);
		};
		let id = tg::tag::Id::try_from(arg.resource.clone())?;
		let tag = Self::try_get_tag_with_transaction(db, subspace, transaction, &id)?;
		Ok(tag.is_some_and(|tag| tag.version == *version))
	}
}
