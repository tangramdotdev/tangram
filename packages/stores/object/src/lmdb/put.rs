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
	pub stored_at: i64,
}

impl Store {
	pub(super) async fn put(&self, arg: PutArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = super::Request::Put(Request {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			stored_at: arg.stored_at,
		});
		self.sender
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!(%id, "the task panicked"))?
	}

	pub(super) async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = super::Request::PutBatch(
			args.into_iter()
				.map(|arg| Request {
					bytes: arg.bytes,
					cache_pointer: arg.cache_pointer,
					id: arg.id,
					stored_at: arg.stored_at,
				})
				.collect(),
		);
		self.sender
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
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
			stored_at: arg.stored_at,
		};
		Self::task_put_object(&self.db, &mut transaction, request)?;
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
				stored_at: arg.stored_at,
			};
			Self::task_put_object(&self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub(super) fn task_put_object(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Request,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = Key::Object(id);
		let key_bytes = key.pack_to_vec();

		let existing = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
			.and_then(|bytes| Object::deserialize(bytes).ok());

		let bytes = existing
			.as_ref()
			.and_then(|entry| entry.bytes.clone())
			.or(request.bytes.map(|bytes| Cow::Owned(bytes.to_vec())));

		let cache_pointer = request
			.cache_pointer
			.or_else(|| existing.and_then(|entry| entry.cache_pointer));

		let value = Object {
			bytes,
			stored_at: request.stored_at,
			cache_pointer,
		};
		let value_bytes = value.serialize().unwrap();
		db.put(transaction, &key_bytes, &value_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;

		Ok(())
	}
}
