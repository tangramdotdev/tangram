use {
	crate::{Server, compiler::Compiler},
	tangram_client as tg,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn check(&self, mut arg: tg::check::Arg) -> tg::Result<tg::check::Output> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let arg = tg::check::Arg {
				remote: None,
				..arg
			};
			let output = remote.check(arg).await?;
			return Ok(output);
		}

		// Create the compiler.
		let compiler = Compiler::new(self, tokio::runtime::Handle::current());

		// Check the package.
		let diagnostics = compiler.check(vec![arg.module]).await?;

		// Create the output.
		let output = tg::check::Output { diagnostics };

		// Stop and await the compiler.
		compiler.stop();
		compiler.wait().await;

		Ok(output)
	}

	pub(crate) async fn handle_check_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.check(arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
