use {super::Store, crate::PutArg, num::ToPrimitive as _, tangram_client::prelude::*};

impl Store {
	pub(super) async fn put(&self, arg: PutArg) -> tg::Result<()> {
		let id = &arg.id;
		if arg.cache_pointer.is_some() {
			return Err(tg::error!(
				%id,
				"cache pointers are not supported by the scylla object store"
			));
		}
		let bytes = arg.bytes;
		let id_bytes = id.to_bytes().to_vec();
		let length = arg
			.length
			.map(|length| {
				length
					.to_i64()
					.ok_or_else(|| tg::error!(%id, "invalid length"))
			})
			.transpose()?;
		let stored_at = arg.stored_at;
		let params = (bytes, id_bytes, length, stored_at);
		self.session
			.execute_unpaged(&self.statements.put_object, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		Ok(())
	}

	pub(super) async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		if let Some(arg) = args.iter().find(|arg| arg.cache_pointer.is_some()) {
			return Err(tg::error!(
				id = %arg.id,
				"cache pointers are not supported by the scylla object store"
			));
		}
		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(self.statements.put_object.get_consistency().unwrap());
		for _ in &args {
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.statements.put_object.clone(),
			));
		}
		let params = args
			.iter()
			.map(|arg| {
				let id = &arg.id;
				let bytes = arg.bytes.clone();
				let id_bytes = id.to_bytes().to_vec();
				let length = arg
					.length
					.map(|length| {
						length
							.to_i64()
							.ok_or_else(|| tg::error!(%id, "invalid length"))
					})
					.transpose()?;
				let stored_at = arg.stored_at;
				Ok((bytes, id_bytes, length, stored_at))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		self.session
			.batch(&batch, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the batch"))?;
		Ok(())
	}
}
