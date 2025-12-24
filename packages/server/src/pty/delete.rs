use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn delete_pty_with_context(
		&self,
		context: &Context,
		id: &tg::pty::Id,
		arg: tg::pty::delete::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::pty::delete::Arg {
				local: None,
				remotes: None,
			};
			client.delete_pty(id, arg).await.map_err(
				|source| tg::error!(!source, %id, "failed to delete the pty on the remote"),
			)?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		self.ptys.remove(id);

		Ok(())
	}

	pub(crate) async fn handle_delete_pty_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the pty id"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		self.delete_pty_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to delete the pty"))?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
