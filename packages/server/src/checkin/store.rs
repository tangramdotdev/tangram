use {crate::Server, tangram_client::prelude::*, tangram_store::prelude::*};

impl Server {
	pub(super) async fn checkin_store(
		&self,
		args: Vec<crate::store::PutObjectArg>,
		progress: &crate::progress::Handle<super::TaskOutput>,
	) -> tg::Result<()> {
		progress.spinner("storing", "storing");
		self.store
			.put_object_batch(args)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;
		progress.finish("storing");
		Ok(())
	}
}
