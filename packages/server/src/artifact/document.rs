use crate::Server;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn document_artifact(
		&self,
		arg: tg::artifact::document::Arg,
	) -> tg::Result<serde_json::Value> {
		// Handle the remote.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!("the remote does not exist"))?
				.clone();
			let arg = tg::artifact::document::Arg {
				remote: None,
				..arg
			};
			let output = remote.document_artifact(arg).await?;
			return Ok(output);
		}

		// Create the module.
		let module = tg::Module {
			kind: tg::module::Kind::Ts,
			object: tg::module::Object::Object(arg.artifact.clone().into()),
		};

		// Create the compiler.
		let compiler = crate::compiler::Compiler::new(self, tokio::runtime::Handle::current());

		// Document the package.
		let output = compiler.document(&module).await?;

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_document_artifact_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.document_artifact(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
