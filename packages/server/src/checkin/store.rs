use {crate::Session, tangram_client::prelude::*};

impl Session {
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn checkin_store(
		&self,
		args: Vec<crate::store::object::put::Arg>,
		progress: &crate::progress::Handle<super::TaskOutput>,
	) -> tg::Result<()> {
		progress.spinner("storing", "storing");
		self.server
			.put_object_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the objects"))?;
		progress.finish("storing");
		Ok(())
	}
}
