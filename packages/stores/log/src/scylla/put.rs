use {super::Store, crate::PutArg, tangram_client::prelude::*};

impl Store {
	pub(super) async fn put(&self, arg: PutArg) -> tg::Result<()> {
		if arg.bytes.is_empty() {
			return Ok(());
		}

		// Create the values.
		let combined_position = i64::try_from(arg.position)
			.map_err(|_| tg::error!("the combined position is too large"))?;
		let kind = super::kind_for_stream(arg.stream)?;
		let length =
			i32::try_from(arg.bytes.len()).map_err(|_| tg::error!("the log entry is too large"))?;
		let process = arg.process.to_bytes().to_vec();
		let stream_position = i64::try_from(arg.stream_position)
			.map_err(|_| tg::error!("the stream position is too large"))?;
		let entry = (
			arg.bytes,
			combined_position,
			super::ENTRY_KIND,
			length,
			combined_position,
			process.clone(),
			kind,
			stream_position,
			arg.timestamp,
		);
		let stream_position_entry = (
			combined_position,
			kind,
			length,
			stream_position,
			process,
			kind,
			stream_position,
			arg.timestamp,
		);

		// Write the entry and its stream position atomically.
		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(scylla::statement::Consistency::LocalQuorum);
		batch.set_is_idempotent(true);
		batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
			self.statements.put_entry.clone(),
		));
		batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
			self.statements.put_stream_position.clone(),
		));
		self.session
			.batch(&batch, (entry, stream_position_entry))
			.await
			.map_err(
				|error| tg::error!(!error, process = %arg.process, "failed to execute the put batch"),
			)?;

		Ok(())
	}
}
