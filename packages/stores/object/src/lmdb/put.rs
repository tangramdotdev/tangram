use {
	super::{Db, Key, Store},
	crate::{CachePointer, Object, PutArg},
	bytes::Bytes,
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	std::borrow::Cow,
	tangram_client::prelude::*,
};

pub(super) struct Request {
	pub bytes: Option<Bytes>,
	pub cache_pointer: Option<CachePointer>,
	pub id: tg::object::Id,
	pub length: Option<u64>,
	pub stored_at: i64,
}

impl Store {
	pub(super) async fn put(&self, arg: PutArg) -> tg::Result<()> {
		let request = super::request::Request::Put(Request {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			length: arg.length,
			stored_at: arg.stored_at,
		});

		self.send_write_request(request).await
	}

	pub(super) async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = super::request::Request::PutBatch(
			args.into_iter()
				.map(|arg| Request {
					bytes: arg.bytes,
					cache_pointer: arg.cache_pointer,
					id: arg.id,
					length: arg.length,
					stored_at: arg.stored_at,
				})
				.collect(),
		);

		self.send_write_request(request).await
	}

	pub fn put_sync(&self, arg: PutArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let request = Request {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			length: arg.length,
			stored_at: arg.stored_at,
		};
		Self::put_inner_with_transaction(&self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn put_batch_sync(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		for arg in args {
			let request = Request {
				bytes: arg.bytes,
				cache_pointer: arg.cache_pointer,
				id: arg.id,
				length: arg.length,
				stored_at: arg.stored_at,
			};
			Self::put_inner_with_transaction(&self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub(super) fn put_inner_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Request,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = Key::Object(id);
		let key_bytes = key.pack_to_vec();

		let value = Object {
			bytes: request.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			cache_pointer: request.cache_pointer,
			length: request.length,
			stored_at: request.stored_at,
		};
		let value_bytes = value.serialize().unwrap();
		db.put(transaction, &key_bytes, &value_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;

		Ok(())
	}
}
