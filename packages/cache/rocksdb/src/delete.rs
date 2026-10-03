use {
	super::{Cache, Key, object as rocksdb_object},
	foundationdb_tuple::TuplePack as _,
	tangram_cache::object,
	tangram_client::prelude::*,
};

pub(super) struct Request {
	pub id: tg::object::Id,
	pub put: [u8; 16],
}

impl Cache {
	pub(super) async fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		let request = super::request::Request::DeleteObject(Request {
			id: arg.id,
			put: arg.put,
		});
		self.send_write_request(request).await?;
		Ok(())
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
					put: arg.put,
				})
				.collect(),
		);

		self.send_write_request(request).await?;
		Ok(())
	}

	pub fn delete_object_sync(&self, arg: object::delete::Arg) -> tg::Result<()> {
		let mut transaction = self
			.db
			.write_transaction()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let request = Request {
			id: arg.id,
			put: arg.put,
		};
		Self::delete_inner_with_transaction(&mut transaction, request)?;
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
			.db
			.write_transaction()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		for arg in args {
			let request = Request {
				id: arg.id,
				put: arg.put,
			};
			Self::delete_inner_with_transaction(&mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	pub(super) fn delete_inner_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
		request: Request,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = Key::Object(rocksdb_object::Key::Object(id));
		let key_bytes = key.pack_to_vec();

		let Some(bytes) = transaction
			.get(&key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
		else {
			return Ok(());
		};
		let value = rocksdb_object::Value::deserialize(&bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
		if value.object.put == request.put {
			transaction
				.delete(&key_bytes)
				.map_err(|error| tg::error!(!error, %id, "failed to delete the object"))?;
		}

		Ok(())
	}
}
