use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_watch_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::watch::Arg,
	) -> tg::Result<Option<tg::checkpoint::watch::Output>> {
		let Some(checkpoints) = self.checkpoint_state()? else {
			return Ok(None);
		};
		let watch = checkpoints.watch(checkpoint, arg.params);
		let output = tg::checkpoint::watch::Output { watch };
		Ok(Some(output))
	}

	pub(crate) async fn watch_checkpoint_request(
		&self,
		request: http::Request<BoxBody>,
		checkpoint: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg: tg::checkpoint::watch::Arg = arg;
		let Some(output) = self
			.try_watch_checkpoint(checkpoint, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to watch the checkpoint"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		let body = serde_json::to_vec(&output).unwrap();
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.body(BoxBody::with_bytes(body))
			.unwrap();
		Ok(response)
	}
}
