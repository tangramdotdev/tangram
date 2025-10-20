use {
	crate::Server,
	tangram_client as tg,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	#[cfg(not(feature = "compiler"))]
	pub async fn document(&self, _arg: tg::document::Arg) -> tg::Result<serde_json::Value> {
		Err(tg::error!(
			"this version of tangram was not compiled with compiler support"
		))
	}

	#[cfg(feature = "compiler")]
	pub async fn document(&self, mut arg: tg::document::Arg) -> tg::Result<serde_json::Value> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			let arg = tg::document::Arg {
				remote: None,
				..arg
			};
			let output = remote.document(arg).await?;
			return Ok(output);
		}

		// Create the compiler.
		let compiler = self.create_compiler();

		// Document the module.
		let output = compiler.document(&arg.module).await?;

		// Stop and await the compiler.
		compiler.stop();
		compiler.wait().await.unwrap();

		Ok(output)
	}

	pub(crate) async fn handle_document_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.document(arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
