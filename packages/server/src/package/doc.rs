use crate::Server;
use either::Either;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn document_package(
		&self,
		id: &tg::package::Id,
		arg: tg::package::doc::Arg,
	) -> tg::Result<serde_json::Value> {
		// Handle the remote.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!("the remote does not exist"))?
				.clone();
			let arg = tg::package::doc::Arg { remote: None };
			let output = remote.document_package(id, arg).await?;
			return Ok(output);
		}

		// Create the module.
		let module = tg::Module {
			kind: tg::module::Kind::Ts,
			object: Either::Right(id.clone().into()),
		};

		// Create the compiler.
		let compiler = crate::compiler::Compiler::new(self, tokio::runtime::Handle::current());

		// Document the package.
		let output = compiler.doc(&module).await?;

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_document_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.document_package(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
