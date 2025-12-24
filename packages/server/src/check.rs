use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	#[cfg(not(feature = "typescript"))]
	pub(crate) async fn check_with_context(
		&self,
		_context: &Context,
		_arg: tg::check::Arg,
	) -> tg::Result<tg::check::Output> {
		Err(tg::error!(
			"this version of tangram was not compiled with typescript support"
		))
	}

	#[cfg(feature = "typescript")]
	pub(crate) async fn check_with_context(
		&self,
		context: &Context,
		arg: tg::check::Arg,
	) -> tg::Result<tg::check::Output> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the remote client"))?;
			let arg = tg::check::Arg {
				local: None,
				modules: arg.modules,
				remotes: None,
			};
			let output = client
				.check(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check on the remote"))?;
			return Ok(output);
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Create the compiler.
		let compiler = self.create_compiler();

		// Check the package.
		let diagnostics = compiler
			.check(arg.modules)
			.await
			.map_err(|source| tg::error!(!source, "failed to check the modules"))?;

		// Create the output.
		let diagnostics = diagnostics.iter().map(tg::Diagnostic::to_data).collect();
		let output = tg::check::Output { diagnostics };

		// Stop and await the compiler.
		compiler.stop();
		compiler
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "failed to wait for the compiler"))?;

		Ok(output)
	}

	pub(crate) async fn handle_check_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		let output = self
			.check_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check the modules"))?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
