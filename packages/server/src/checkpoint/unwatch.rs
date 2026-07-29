use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, response::Ext as _, response::builder::Ext as _},
};

impl Session {
	pub(crate) async fn try_unwatch_checkpoint(
		&self,
		checkpoint: &str,
		watch: u64,
	) -> tg::Result<Option<()>> {
		let Some(checkpoints) = self.checkpoint_state()? else {
			return Ok(None);
		};
		if !checkpoints.unwatch(checkpoint, watch) {
			return Ok(None);
		}
		Ok(Some(()))
	}

	pub(crate) async fn unwatch_checkpoint_request(
		&self,
		_request: http::Request<BoxBody>,
		checkpoint: &str,
		watch: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let watch = watch
			.parse()
			.map_err(|error| tg::error!(!error, "invalid checkpoint watch"))?;
		let Some(()) = self
			.try_unwatch_checkpoint(checkpoint, watch)
			.await
			.map_err(|error| tg::error!(!error, "failed to remove the checkpoint watch"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
