use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn close_pty_with_context(
		&self,
		context: &Context,
		id: &tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::pty::close::Arg {
				local: None,
				master: arg.master,
				remotes: None,
			};
			client.close_pty(id, arg).await.map_err(
				|source| tg::error!(!source, %id, "failed to close the pty on the remote"),
			)?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let mut pty = self
			.ptys
			.get_mut(id)
			.ok_or_else(|| tg::error!("failed to get the pty"))?;
		if arg.master {
			pty.master.take();
		} else {
			pty.session.take();
			pty.slave.take();
		}
		drop(pty);

		Ok(())
	}

	pub(crate) async fn handle_close_pty_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the pty id"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
		self.close_pty_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to close the pty"))?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
