use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	#[cfg(not(feature = "compiler"))]
	pub(crate) async fn check_with_context(
		&self,
		context: &Context,
		_arg: tg::check::Arg,
	) -> tg::Result<tg::check::Output> {
		if context.proxy.is_some() {
			return Err(tg::error!("forbidden"));
		}
		Err(tg::error!(
			"this version of tangram was not compiled with compiler support"
		))
	}

	#[cfg(feature = "compiler")]
	pub(crate) async fn check_with_context(
		&self,
		context: &Context,
		mut arg: tg::check::Arg,
	) -> tg::Result<tg::check::Output> {
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

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Create the compiler.
		let compiler = self.create_compiler();

		// Check the package.
		let diagnostics = compiler.check(vec![arg.module]).await?;

		// Create the output.
		let diagnostics = diagnostics.iter().map(tg::Diagnostic::to_data).collect();
		let output = tg::check::Output { diagnostics };

		// Stop and await the compiler.
		compiler.stop();
		compiler.wait().await.unwrap();

		Ok(output)
	}

	pub(crate) async fn handle_check_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let arg = request.json().await?;
		let output = self.check_with_context(context, arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
