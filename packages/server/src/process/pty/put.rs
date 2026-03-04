use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _}, tangram_messenger::Messenger,
};

impl Server {
	pub(crate) async fn try_put_process_pty_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::pty::put::Arg,
	) -> tg::Result<()> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::process::pty::put::Arg {
				local: None,
				remotes: None,
				size: arg.size,
			};
			return client.put_process_pty(id, arg).await;
		}

		// Check if the process has a pty
		if tg::Process::new(id.clone(), None, None, None, None)
			.load(self)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to load the process"))?
			.pty
			.is_none()
		{
			return Err(tg::error!(%id, "the process does not have a pty associated with it"));
		}

		// Publish the message.
		let event = tg::process::pty::get::Event::Pty(tg::process::Pty { size: arg.size });
		let payload =
			tangram_messenger::payload::Json(event);
		self.messenger
			.publish(format!("processes.{id}.pty"), payload)
			.await
			.map_err(|source| tg::error!(!source, "failed to update the pty size"))?;
		
		Ok(())
	}

	pub(crate) async fn handle_put_process_pty_request(
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

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|_| tg::error!("failed to deserialize the arg"))?;

		// Put the process pty.
		self.try_put_process_pty_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the process pty"))?;

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
