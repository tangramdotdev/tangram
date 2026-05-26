use {super::Store, crate::DeleteArg, num::ToPrimitive as _, tangram_client::prelude::*};

impl Store {
	pub(super) async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		let id = &arg.id;
		let id_bytes = id.to_bytes().to_vec();
		let max_stored_at = arg.now - arg.ttl.to_i64().unwrap();
		let params = (id_bytes.clone(), max_stored_at);
		let result = self
			.session
			.execute_unpaged(&self.statements.delete_object, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, %id, "failed to get the rows"))?;
		let Some((applied,)) = result
			.maybe_first_row::<(bool,)>()
			.map_err(|error| tg::error!(!error, %id, "failed to get the row"))?
		else {
			return Ok(());
		};
		if applied {
			self.session
				.execute_unpaged(&self.statements.delete_object_grants, (id_bytes,))
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		}
		Ok(())
	}

	pub(super) async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		futures::future::try_join_all(args.into_iter().map(|arg| self.delete(arg))).await?;
		Ok(())
	}
}
