use {
	super::Store,
	crate::object,
	futures::{TryStreamExt as _, stream},
	tangram_client::prelude::*,
};

impl Store {
	pub(super) async fn put_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		let id = &arg.id;
		if arg.checkout_pointer.is_some() {
			return Err(tg::error!(
				%id,
				"checkout pointers are not supported by the scylla store"
			));
		}
		let bytes = arg.bytes;
		let id_bytes = id.to_bytes().to_vec();
		let stored_at = arg.stored_at;
		let timestamp = super::object_timestamp(stored_at)?;
		let params = (bytes, id_bytes, stored_at, timestamp);
		self.session
			.execute_unpaged(&self.statements.put_object, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		Ok(())
	}

	pub(super) async fn put_object_batch(&self, args: Vec<object::put::Arg>) -> tg::Result<()> {
		if let Some(arg) = args.iter().find(|arg| arg.checkout_pointer.is_some()) {
			return Err(tg::error!(
				id = %arg.id,
				"checkout pointers are not supported by the scylla store"
			));
		}
		stream::iter(args.into_iter().map(Ok))
			.try_for_each_concurrent(super::OBJECT_CONCURRENCY, |arg| self.put_object(arg))
			.await?;

		Ok(())
	}
}
