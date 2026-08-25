use {super::Store, crate::DeleteArg, tangram_client::prelude::*};

impl Store {
	pub(super) async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		let process = arg.process.to_bytes().to_vec();
		self.session
			.execute_unpaged(&self.statements.delete, (process,))
			.await
			.map_err(
				|error| tg::error!(!error, process = %arg.process, "failed to execute the delete query"),
			)?;

		Ok(())
	}
}
