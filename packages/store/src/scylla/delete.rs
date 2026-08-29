use {
	super::Store,
	crate::object,
	futures::{TryStreamExt as _, stream},
	tangram_client::prelude::*,
};

impl Store {
	pub(super) async fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		let id = &arg.id;
		let id_bytes = id.to_bytes().to_vec();
		let timestamp = object::cache::stored_at_timestamp(arg.touched_at)?;
		let params = (timestamp, id_bytes);
		self.session
			.execute_unpaged(&self.statements.delete_object, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		Ok(())
	}

	pub(super) async fn delete_object_batch(
		&self,
		args: Vec<object::delete::Arg>,
	) -> tg::Result<()> {
		stream::iter(args.into_iter().map(Ok))
			.try_for_each_concurrent(super::OBJECT_CONCURRENCY, |arg| self.delete_object(arg))
			.await?;

		Ok(())
	}
}
