use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, response::Ext as _, response::builder::Ext as _},
};

impl Session {
	pub(crate) async fn try_continue_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> tg::Result<Option<()>> {
		let Some(checkpoints) = self.checkpoint_state()? else {
			return Ok(None);
		};
		if !checkpoints.continue_hit(checkpoint, watch, hit) {
			return Ok(None);
		}
		Ok(Some(()))
	}

	pub(crate) async fn continue_checkpoint_hit_request(
		&self,
		_request: http::Request<BoxBody>,
		checkpoint: &str,
		watch: &str,
		hit: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let watch = watch
			.parse()
			.map_err(|error| tg::error!(!error, "invalid checkpoint watch"))?;
		let hit = hit
			.parse()
			.map_err(|error| tg::error!(!error, "invalid checkpoint hit"))?;
		let Some(()) = self
			.try_continue_checkpoint_hit(checkpoint, watch, hit)
			.await
			.map_err(|error| tg::error!(!error, "failed to continue the checkpoint hit"))?
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
