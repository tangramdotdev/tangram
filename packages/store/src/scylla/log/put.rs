use {super::super::Store, crate::log, scylla::value::MaybeUnset, tangram_client::prelude::*};

impl Store {
	pub(in crate::scylla) async fn put_log_inner(&self, arg: log::put::Arg) -> tg::Result<()> {
		self.put_log_batch_inner(vec![arg]).await
	}

	pub(in crate::scylla) async fn put_log_batch_inner(
		&self,
		mut args: Vec<log::put::Arg>,
	) -> tg::Result<()> {
		args.retain(|arg| !arg.bytes.is_empty());
		let Some(process) = args.first().map(|arg| arg.process.clone()) else {
			return Ok(());
		};
		if args.iter().any(|arg| arg.process != process) {
			return Err(tg::error!("expected log entries for one process"));
		}

		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(scylla::statement::Consistency::LocalQuorum);
		batch.set_is_idempotent(true);
		let mut values = Vec::with_capacity(args.len() * 2);
		for arg in args {
			let combined_position = i64::try_from(arg.position)
				.map_err(|_| tg::error!("the combined position is too large"))?;
			let kind = super::kind_for_stream(arg.stream)?;
			let length = i32::try_from(arg.bytes.len())
				.map_err(|_| tg::error!("the log entry is too large"))?;
			let process = arg.process.to_bytes().to_vec();
			let stream_position = i64::try_from(arg.stream_position)
				.map_err(|_| tg::error!("the stream position is too large"))?;
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.statements.log.put.clone(),
			));
			values.push((
				MaybeUnset::Set(arg.bytes),
				combined_position,
				super::ENTRY_KIND,
				length,
				combined_position,
				process.clone(),
				kind,
				stream_position,
				arg.timestamp,
			));
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.statements.log.put.clone(),
			));
			values.push((
				MaybeUnset::Unset,
				combined_position,
				kind,
				length,
				stream_position,
				process,
				kind,
				stream_position,
				arg.timestamp,
			));
		}
		self.session
			.batch(&batch, values)
			.await
			.map_err(|error| tg::error!(!error, %process, "failed to execute the put batch"))?;

		Ok(())
	}
}
