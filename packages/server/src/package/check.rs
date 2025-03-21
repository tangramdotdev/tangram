use crate::{Server, compiler::Compiler};
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn check_package(
		&self,
		mut arg: tg::package::check::Arg,
	) -> tg::Result<tg::package::check::Output> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let arg = tg::package::check::Arg {
				remote: None,
				..arg
			};
			let output = remote.check_package(arg).await?;
			return Ok(output);
		}

		// Create the compiler.
		let compiler = Compiler::new(self, tokio::runtime::Handle::current());

		// Create the module.
		let module = arg.package;

		// Check the package.
		let diagnostics = compiler.check(vec![module]).await?;

		// Create the output.
		let output = tg::package::check::Output { diagnostics };

		// Stop and await the compiler.
		compiler.stop();
		compiler.wait().await;

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_check_package_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.check_package(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
