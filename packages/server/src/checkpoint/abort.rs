use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_abort_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::abort::Arg,
	) -> tg::Result<Option<()>> {
		let Some(checkpoints) = self.checkpoint_state()? else {
			return Ok(None);
		};
		checkpoints.abort(checkpoint, arg.params);
		Ok(Some(()))
	}

	pub(crate) async fn abort_checkpoint_request(
		&self,
		request: http::Request<BoxBody>,
		checkpoint: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg: tg::checkpoint::abort::Arg = arg;
		let Some(()) = self
			.try_abort_checkpoint(checkpoint, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to register the checkpoint abort"))?
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
