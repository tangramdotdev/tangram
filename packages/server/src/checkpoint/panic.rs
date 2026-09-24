use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_panic_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::panic::Arg,
	) -> tg::Result<Option<()>> {
		let Some(checkpoints) = self.checkpoint_state()? else {
			return Ok(None);
		};
		checkpoints.panic(checkpoint, arg.params);
		Ok(Some(()))
	}

	pub(crate) async fn panic_checkpoint_request(
		&self,
		request: http::Request<BoxBody>,
		checkpoint: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg: tg::checkpoint::panic::Arg = arg;
		let Some(()) = self
			.try_panic_checkpoint(checkpoint, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to register the checkpoint panic"))?
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
