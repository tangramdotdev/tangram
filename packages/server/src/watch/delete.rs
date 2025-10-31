use {
	crate::{Server, handle::ServerOrProxy},
	tangram_client::{self as tg, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn delete_watch(&self, mut arg: tg::watch::delete::Arg) -> tg::Result<()> {
		// Canonicalize the path's parent.
		arg.path = tangram_util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, %path = arg.path.display(), "failed to canonicalize the path's parent"))?;

		self.watches.remove(&arg.path);

		Ok(())
	}

	pub(crate) async fn handle_delete_watch_request(
		handle: &ServerOrProxy,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	{
		let arg = request
			.query_params()
			.transpose()?
			.ok_or_else(|| tg::error!("missing query params"))?;
		handle.delete_watch(arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
