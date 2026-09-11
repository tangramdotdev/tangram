use {super::super::Cache, crate::log, tangram_client::prelude::*};

impl Cache {
	pub(in crate::scylla) async fn put_log_end_inner(&self, arg: log::end::Arg) -> tg::Result<()> {
		let process = arg.process.to_bytes().to_vec();
		let position = i64::try_from(arg.end.position)
			.map_err(|_| tg::error!("the log position is too large"))?;
		let bytes = tangram_serialize::to_vec(&arg.end)
			.map_err(|error| tg::error!(!error, "failed to serialize the log end"))?;
		self.session
			.execute_unpaged(
				&self.statements.log.put_end,
				(process, super::END_KIND, position, bytes),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to cache the log end"))?;
		Ok(())
	}

	pub(in crate::scylla) async fn try_get_log_end_inner(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::log::End>> {
		let process = process.to_bytes().to_vec();
		let result = self
			.session
			.execute_unpaged(&self.statements.log.get_end, (process, super::END_KIND))
			.await
			.map_err(|error| tg::error!(!error, "failed to get the log end"))?;
		let rows = result
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the log end rows"))?;
		let row = rows
			.maybe_first_row::<(Vec<u8>,)>()
			.map_err(|error| tg::error!(!error, "failed to read the log end row"))?;
		let output = row
			.map(|(bytes,)| tangram_serialize::from_slice(&bytes))
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to deserialize the log end"))?;
		Ok(output)
	}
}
