use crate::{Server, compiler::Compiler};
use tangram_client as tg;
use tangram_either::Either;
use tangram_http::{Incoming, Outgoing, incoming::request::Ext as _, outgoing::response::Ext as _};

impl Server {
	pub async fn document_package(
		&self,
		arg: tg::package::document::Arg,
	) -> tg::Result<serde_json::Value> {
		// If the remote arg is set, then forward the request.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!("the remote does not exist"))?
				.clone();
			let arg = tg::package::document::Arg {
				remote: None,
				..arg
			};
			let output = remote.document_package(arg).await?;
			return Ok(output);
		}

		// Create the compiler.
		let compiler = Compiler::new(self, tokio::runtime::Handle::current());

		// Create the module.
		let module = self
			.root_module_for_package(Either::Left(arg.package))
			.await?;

		// Document the module.
		let output = compiler.document(&module).await?;

		// Stop the compiler.
		compiler.stop().await;

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_document_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.document_package(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
