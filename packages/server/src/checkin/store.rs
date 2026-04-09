use {crate::Server, tangram_client::prelude::*, tangram_object_store::prelude::*};

impl Server {
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn checkin_store(
		&self,
		args: Vec<crate::object::store::PutObjectArg>,
		progress: &crate::progress::Handle<super::TaskOutput>,
	) -> tg::Result<()> {
		progress.spinner("storing", "storing");
		self.object_store
			.put_object_batch(args)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;
		progress.finish("storing");
		Ok(())
	}
}
