use crate::Server;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn check_package(
		&self,
		arg: tg::package::check::Arg,
	) -> tg::Result<tg::package::check::Output> {
		// Handle the remote.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!("the remote does not exist"))?
				.clone();
			let arg = tg::package::check::Arg {
				remote: None,
				..arg
			};
			let output = remote.check_package(arg).await?;
			return Ok(output);
		}

		// Create the module.
		let module = tg::Module {
			kind: tg::module::Kind::Ts,
			object: tg::module::Object::Object(arg.package.clone().into()),
		};

		// Create the compiler.
		let compiler = crate::compiler::Compiler::new(self, tokio::runtime::Handle::current());

		// Check the package.
		let diagnostics = compiler.check(vec![module]).await?;

		// Create the output.
		let output = tg::package::check::Output { diagnostics };

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_check_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.check_package(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
