use {super::Store, crate::PutArg, tangram_client::prelude::*};

impl Store {
	pub(super) async fn put(&self, arg: PutArg) -> tg::Result<()> {
		let id = &arg.id;
		let bytes = arg.bytes;
		let cache_pointer = if let Some(cache_pointer) = &arg.cache_pointer {
			let cache_pointer = cache_pointer.serialize().map_err(
				|error| tg::error!(!error, %id, "failed to serialize the cache pointer"),
			)?;
			Some(cache_pointer)
		} else {
			None
		};
		let id_bytes = id.to_bytes().to_vec();
		let stored_at = arg.stored_at;
		let params = (bytes, cache_pointer, id_bytes, stored_at);
		self.session
			.execute_unpaged(&self.statements.put_object, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		if let Some(principal) = arg.principal {
			self.put_grant(id, principal, false, stored_at).await?;
		}
		Ok(())
	}

	pub(super) async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
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
				let cache_pointer = if let Some(cache_pointer) = &arg.cache_pointer {
					let cache_pointer = cache_pointer.serialize().map_err(
						|error| tg::error!(!error, %id, "failed to serialize the cache pointer"),
					)?;
					Some(cache_pointer)
				} else {
					None
				};
				let id_bytes = id.to_bytes().to_vec();
				let stored_at = arg.stored_at;
				let params = (bytes, cache_pointer, id_bytes, stored_at);
				Ok(params)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		self.session
			.batch(&batch, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the batch"))?;
		futures::future::try_join_all(args.iter().filter_map(|arg| {
			arg.principal
				.clone()
				.map(|principal| self.put_grant(&arg.id, principal, false, arg.stored_at))
		}))
		.await?;
		Ok(())
	}
}
