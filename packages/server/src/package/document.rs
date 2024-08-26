use crate::{compiler::Compiler, Server};
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn document_package(
		&self,
		arg: tg::package::document::Arg,
	) -> tg::Result<serde_json::Value> {
		// Handle the remote.
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

		// Get a reference to the package's root module.
		let package = tg::Directory::with_id(arg.package.clone());
		let module = tg::module::Reference::with_package(self, &package).await?;

		// Create the compiler.
		let compiler = Compiler::new(self, tokio::runtime::Handle::current());

		// Document the package.
		let output = compiler.document(&module).await?;

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
