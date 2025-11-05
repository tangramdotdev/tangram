use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn delete_watch_with_context(
		&self,
		context: &Context,
		mut arg: tg::watch::delete::Arg,
	) -> tg::Result<()> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Canonicalize the path's parent.
		arg.path = tangram_util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, %path = arg.path.display(), "failed to canonicalize the path's parent"))?;

		self.watches.remove(&arg.path);

		Ok(())
	}

	pub(crate) async fn handle_delete_watch_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let arg = request
			.query_params()
			.transpose()?
			.ok_or_else(|| tg::error!("missing query params"))?;
		self.delete_watch_with_context(context, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
