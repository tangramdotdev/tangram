use {super::super::Store, crate::log, tangram_client::prelude::*};

impl Store {
	pub(in crate::scylla) async fn delete_log_inner(
		&self,
		arg: log::delete::Arg,
	) -> tg::Result<()> {
		let process = arg.process.to_bytes().to_vec();
		self.session
			.execute_unpaged(&self.statements.log.delete, (process,))
			.await
			.map_err(
				|error| tg::error!(!error, process = %arg.process, "failed to execute the delete query"),
			)?;

		Ok(())
	}
}
