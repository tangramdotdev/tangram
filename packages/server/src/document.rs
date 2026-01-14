use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _},
};

impl Server {
	#[cfg(not(feature = "typescript"))]
	pub(crate) async fn document_with_context(
		&self,
		_context: &Context,
		_arg: tg::document::Arg,
	) -> tg::Result<serde_json::Value> {
		Err(tg::error!(
			"this version of tangram was not compiled with typescript support"
		))
	}

	#[cfg(feature = "typescript")]
	pub(crate) async fn document_with_context(
		&self,
		context: &Context,
		arg: tg::document::Arg,
	) -> tg::Result<serde_json::Value> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the remote client"))?;
			let arg = tg::document::Arg {
				local: None,
				module: arg.module,
				remotes: None,
			};
			let output = client
				.document(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to document on the remote"))?;
			return Ok(output);
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Create the compiler.
		let compiler = self.create_compiler();

		// Document the module.
		let output = compiler
			.document(&arg.module)
			.await
			.map_err(|source| tg::error!(!source, "failed to document the module"))?;

		// Stop and await the compiler.
		compiler.stop();
		compiler
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "failed to wait for the compiler"))?;

		Ok(output)
	}

	pub(crate) async fn handle_document_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Document the module.
		let output = self
			.document_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to document the module"))?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), Body::with_bytes(body))
			},
			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();
		Ok(response)
	}
}
