use {crate::Server, tangram_client as tg, tangram_store::prelude::*};

impl Server {
	pub(super) async fn checkin_store(&self, args: Vec<crate::store::PutArg>) -> tg::Result<()> {
		self.store
			.put_batch(args)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;
		Ok(())
	}
}
