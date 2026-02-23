use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Server {
	pub(crate) async fn delete_sandbox_with_context(
		&self,
		context: &Context,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Remove the sandbox from the map.
		let (_, mut sandbox) = self
			.sandboxes
			.remove(id)
			.ok_or_else(|| tg::error!("the sandbox was not found"))?;

		// Kill and wait for the sandbox process.
		sandbox
			.process
			.kill()
			.await
			.map_err(|source| tg::error!(!source, "failed to kill the sandbox process"))?;

		Ok(())
	}

	pub(crate) async fn handle_delete_sandbox_request(
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

		// Parse the sandbox id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the sandbox id"))?;

		// Delete the sandbox.
		self.delete_sandbox_with_context(context, &id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to delete the sandbox"))?;

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
