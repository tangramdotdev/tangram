use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::request::Ext as _,
	tokio::io::AsyncWriteExt as _,
};

impl Server {
	pub(crate) async fn close_pipe_with_context(
		&self,
		context: &Context,
		id: &tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::pipe::close::Arg {
				local: None,
				remotes: None,
			};
			client.close_pipe(id, arg).await.map_err(
				|source| tg::error!(!source, %id, "failed to close the pipe on the remote"),
			)?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let mut pipe = self
			.pipes
			.get_mut(id)
			.ok_or_else(|| tg::error!("failed to find the pipe"))?;
		let mut sender = pipe
			.sender
			.take()
			.ok_or_else(|| tg::error!("the pipe is already closed"))?;
		sender
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush the pipe"))?;

		Ok(())
	}

	pub(crate) async fn handle_close_pipe_request(
		&self,
		request: tangram_http::Request,
		context: &Context,
		id: &str,
	) -> tg::Result<tangram_http::Response> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the pipe id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the pipe id"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Close the pipe.
		self.close_pipe_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to close the pipe"))?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder()
			.body(tangram_http::body::Boxed::empty())
			.unwrap();
		Ok(response)
	}
}
