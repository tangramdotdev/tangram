use {super::Store, crate::object, num::ToPrimitive as _, tangram_client::prelude::*};

impl Store {
	pub(super) async fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		let id = &arg.id;
		let id_bytes = id.to_bytes().to_vec();
		let max_stored_at = arg.now - arg.ttl.to_i64().unwrap();
		let params = (id_bytes, max_stored_at);
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
		futures::future::try_join_all(args.into_iter().map(|arg| self.delete_object(arg))).await?;
		Ok(())
	}
}
