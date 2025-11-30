use {crate::Server, tangram_client::prelude::*, tangram_store::prelude::*};

impl Server {
	pub(super) async fn checkin_store(
		&self,
		args: Vec<crate::store::PutArg>,
		progress: &crate::progress::Handle<tg::checkin::Output>,
	) -> tg::Result<()> {
		progress.spinner("storing", "storing");
		self.store
			.put_batch(args)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;
		progress.finish("storing");
		Ok(())
	}
}
