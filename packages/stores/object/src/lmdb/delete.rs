use {
	super::{Db, Key, Store},
	crate::{DeleteArg, Object},
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
	pub(super) async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = super::Request::Delete(Request {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		});
		self.sender
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!(%id, "the task panicked"))?
	}

	pub(super) async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = super::Request::DeleteBatch(
			args.into_iter()
				.map(|arg| Request {
					id: arg.id,
					now: arg.now,
					ttl: arg.ttl,
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

	pub fn delete_sync(&self, arg: DeleteArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let request = Request {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		};
		Self::task_delete_object(&self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_batch_sync(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
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
			Self::task_delete_object(&self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	pub(super) fn task_delete_object(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Request,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = Key::Object(id);
		let key_bytes = key.pack_to_vec();

		let Some(bytes) = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
		else {
			return Ok(());
		};
		let value = Object::deserialize(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;

		if request.now - value.stored_at >= request.ttl.to_i64().unwrap() {
			db.delete(transaction, &key_bytes)
				.map_err(|error| tg::error!(!error, %id, "failed to delete the object"))?;
		}

		Ok(())
	}
}
