use {crate::Component, tangram_client::prelude::*, tangram_server_core::Session};

impl Component {
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn checkin_store(
		&self,
		session: &Session,
		args: Vec<tangram_cache::object::put::Arg>,
		progress: &tg::progress::Handle<super::TaskOutput>,
	) -> tg::Result<()> {
		progress.spinner("storing", "storing");
		session
			.server
			.components
			.object
			.put_object_batch(&session.server, args)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the objects"))?;
		progress.finish("storing");
		Ok(())
	}
}
