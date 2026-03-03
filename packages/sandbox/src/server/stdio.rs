use {
	crate::server::Server,
	tangram_client::prelude::*,
	tangram_futures::{BoxAsyncRead, read::Ext as _},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext, builder::Ext as _},
	},
};

impl Server {
	pub async fn stdin(
		&self,
		arg: crate::client::stdio::StdinArg,
		stdin: BoxAsyncRead<'static>,
	) -> tg::Result<()> {
		todo!()
	}

	pub(crate) async fn handle_stdin(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("missing query params"))?;

		// Get stdin.
		let stdin = request.reader().boxed();

		// Write stdin.
		self.stdin(arg, stdin)
			.await
			.map_err(|source| tg::error!(!source, "failed to handle stdin"))?;

		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	pub async fn stdout(
		&self,
		arg: crate::client::stdio::StdoutArg,
	) -> tg::Result<BoxAsyncRead<'static>> {
		todo!()
	}

	pub(crate) async fn handle_stdout(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("missing query params"))?;

		// Get stdout.
		let stdout = self
			.stdout(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to handle stdout"))?;

		let response = http::Response::builder()
			.body(BoxBody::with_reader(stdout))
			.unwrap();
		Ok(response)
	}

	pub async fn stderr(
		&self,
		arg: crate::client::stdio::StderrArg,
	) -> tg::Result<BoxAsyncRead<'static>> {
		todo!()
	}

	pub(crate) async fn handle_stderr(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("missing query params"))?;

		// Get stderr.
		let stderr = self
			.stderr(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to handle stderr"))?;

		let response = http::Response::builder()
			.body(BoxBody::with_reader(stderr))
			.unwrap();
		Ok(response)
	}
}
