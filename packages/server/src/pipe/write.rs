use {
	crate::{Context, Server},
	tangram_client as tg,
	tangram_http::{Body, request::Ext as _},
	tokio::io::AsyncWriteExt as _,
};

impl Server {
	pub async fn write_pipe_with_context(
		&self,
		_context: &Context,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::pipe::write::Arg {
				bytes: arg.bytes,
				local: None,
				remotes: None,
			};
			return client
				.write_pipe(id, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to write to the pipe on remote"));
		}

		let mut pipe = self
			.pipes
			.get_mut(id)
			.ok_or_else(|| tg::error!("failed to find the pipe"))?;
		pipe.sender
			.as_mut()
			.ok_or_else(|| tg::error!("the pipe is closed"))?
			.write_all(&arg.bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the pipe"))?;
		Ok(())
	}

	pub(crate) async fn handle_write_pipe_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Post the process log.
		self.write_pipe_with_context(context, &id, arg).await?;

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

		let response = http::Response::builder().body(Body::empty()).unwrap();
		Ok(response)
	}
}
