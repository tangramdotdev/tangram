use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Server {
	pub(crate) async fn delete_pipe_with_context(
		&self,
		context: &Context,
		id: &tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> tg::Result<()> {
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::pipe::delete::Arg {
				local: None,
				remotes: None,
			};
			client.delete_pipe(id, arg).await.map_err(
				|source| tg::error!(!source, %id, "failed to delete the pipe on the remote"),
			)?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		self.pipes.remove(id);
		Ok(())
	}

	pub(crate) async fn handle_delete_pipe_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
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
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Delete the pipe.
		self.delete_pipe_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to delete the pipe"))?;

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

		let response = http::Response::builder().body(BoxBody::empty()).unwrap();
		Ok(response)
	}
}
