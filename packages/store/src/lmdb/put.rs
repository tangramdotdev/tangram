use {
	super::{Db, Key, Store, object as lmdb_object},
	crate::object,
	bytes::Bytes,
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	std::borrow::Cow,
	tangram_client::prelude::*,
};

pub(super) struct Request {
	pub bytes: Option<Bytes>,
	pub checkout_pointer: Option<object::checkout::Pointer>,
	pub id: tg::object::Id,
	pub length: Option<u64>,
	pub stored_at: i64,
}

impl Store {
	pub(super) async fn put_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		let request = super::request::Request::PutObject(Request {
			bytes: arg.bytes,
			checkout_pointer: arg.checkout_pointer,
			id: arg.id,
			length: arg.length,
			stored_at: arg.stored_at,
		});

		self.send_write_request(request).await
	}

	pub(super) async fn put_object_batch(&self, args: Vec<object::put::Arg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = super::request::Request::PutObjectBatch(
			args.into_iter()
				.map(|arg| Request {
					bytes: arg.bytes,
					checkout_pointer: arg.checkout_pointer,
					id: arg.id,
					length: arg.length,
					stored_at: arg.stored_at,
				})
				.collect(),
		);

		self.send_write_request(request).await
	}

	pub fn put_object_sync(&self, arg: object::put::Arg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let request = Request {
			bytes: arg.bytes,
			checkout_pointer: arg.checkout_pointer,
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

	pub fn put_object_batch_sync(&self, args: Vec<object::put::Arg>) -> tg::Result<()> {
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
				checkout_pointer: arg.checkout_pointer,
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
		let key = Key::Object(lmdb_object::Key::Object(id));
		let key_bytes = key.pack_to_vec();
		let timestamp = object::cache::stored_at_timestamp(request.stored_at)?;
		let previous = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
			.map(super::object::Value::deserialize)
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
		if previous.is_some_and(|object| object.timestamp > timestamp) {
			return Ok(());
		}

		let value = object::Object {
			bytes: request.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			checkout_pointer: request.checkout_pointer,
			length: request.length,
			stored_at: request.stored_at,
		};
		let value = super::object::Value::new(value, timestamp);
		let value_bytes = value.serialize()?;
		db.put(transaction, &key_bytes, &value_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;

		Ok(())
	}
}
