use {
	super::{Cache, Key, object as fjall_object},
	bytes::Bytes,
	foundationdb_tuple::TuplePack as _,
	std::borrow::Cow,
	tangram_cache::object,
	tangram_client::prelude::*,
};

pub(super) struct Request {
	pub bytes: Option<Bytes>,
	pub checkout_pointer: Option<object::checkout::Pointer>,
	pub id: tg::object::Id,
	pub length: Option<u64>,
	pub put: [u8; 16],
}

impl Cache {
	pub(super) async fn put_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		let request = super::request::Request::PutObject(Request {
			bytes: arg.bytes,
			checkout_pointer: arg.checkout_pointer,
			id: arg.id,
			length: arg.length,
			put: arg.put,
		});
		self.send_write_request(request).await?;
		Ok(())
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
					put: arg.put,
				})
				.collect(),
		);

		self.send_write_request(request).await?;
		Ok(())
	}

	pub fn put_object_sync(&self, arg: object::put::Arg) -> tg::Result<()> {
		let mut transaction = self.db.write_transaction();
		let request = Request {
			bytes: arg.bytes,
			checkout_pointer: arg.checkout_pointer,
			id: arg.id,
			length: arg.length,
			put: arg.put,
		};
		Self::put_inner_with_transaction(&mut transaction, request)?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn put_object_batch_sync(&self, args: Vec<object::put::Arg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self.db.write_transaction();
		for arg in args {
			let request = Request {
				bytes: arg.bytes,
				checkout_pointer: arg.checkout_pointer,
				id: arg.id,
				length: arg.length,
				put: arg.put,
			};
			Self::put_inner_with_transaction(&mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub(super) fn put_inner_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
		request: Request,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = Key::Object(fjall_object::Key::Object(id));
		let key_bytes = key.pack_to_vec();
		let previous = transaction
			.get(&key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
			.map(|bytes| super::object::Value::deserialize(&bytes))
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
		if previous.is_some_and(|object| object.object.put > request.put) {
			return Ok(());
		}

		let value = object::Object {
			bytes: request.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			checkout_pointer: request.checkout_pointer,
			length: request.length,
			put: request.put,
		};
		let value = super::object::Value::new(value);
		let value_bytes = value.serialize()?;
		transaction
			.put(&key_bytes, &value_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;

		Ok(())
	}
}
