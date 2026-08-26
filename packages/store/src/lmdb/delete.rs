use {
	super::{Db, Key, Store, object as lmdb_object},
	crate::object,
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

pub(super) struct Request {
	pub id: tg::object::Id,
	pub now: i64,
	pub ttl: u64,
}

impl Store {
	pub(super) async fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		let request = super::request::Request::DeleteObject(Request {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		});

		self.send_write_request(request).await
	}

	pub(super) async fn delete_object_batch(
		&self,
		args: Vec<object::delete::Arg>,
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = super::request::Request::DeleteObjectBatch(
			args.into_iter()
				.map(|arg| Request {
					id: arg.id,
					now: arg.now,
					ttl: arg.ttl,
				})
				.collect(),
		);

		self.send_write_request(request).await
	}

	pub fn delete_object_sync(&self, arg: object::delete::Arg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let request = Request {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		};
		Self::delete_inner_with_transaction(&self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_object_batch_sync(&self, args: Vec<object::delete::Arg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		for arg in args {
			let request = Request {
				id: arg.id,
				now: arg.now,
				ttl: arg.ttl,
			};
			Self::delete_inner_with_transaction(&self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	pub(super) fn delete_inner_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Request,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = Key::Object(lmdb_object::Key::Object(id));
		let key_bytes = key.pack_to_vec();

		let Some(bytes) = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
		else {
			return Ok(());
		};
		let value = object::Object::deserialize(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;

		if request.now - value.stored_at >= request.ttl.to_i64().unwrap() {
			db.delete(transaction, &key_bytes)
				.map_err(|error| tg::error!(!error, %id, "failed to delete the object"))?;
		}

		Ok(())
	}
}
