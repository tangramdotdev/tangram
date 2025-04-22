use crate::{Server, compiler::Compiler};
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn document(
		&self,
		mut arg: tg::document::Arg,
	) -> tg::Result<serde_json::Value> {
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
		let compiler = Compiler::new(self, tokio::runtime::Handle::current());

		// Document the module.
		let output = compiler.document(&arg.module).await?;

		// Stop and await the compiler.
		compiler.stop();
		compiler.wait().await;

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
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
