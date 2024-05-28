use crate::Server;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tg::Handle as _;

impl Server {
	pub async fn check_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::check::Arg,
	) -> tg::Result<Vec<tg::Diagnostic>> {
		// Get the package.
		let arg = tg::package::get::Arg {
			lock: true,
			locked: arg.locked,
			..Default::default()
		};
		let output = self.get_package(&dependency, arg).await?;
		let package = tg::Artifact::with_id(output.artifact);
		let lock = output
			.lock
			.ok_or_else(|| tg::error!("expected the lock to be set"))?;
		let lock = tg::Lock::with_id(lock);

		// Create the root module.
		let module = tg::Module::with_package_and_lock(self, &package, &lock).await?;

		// Create the compiler.
		let compiler = crate::compiler::Compiler::new(self, tokio::runtime::Handle::current());

		// Get the diagnostics.
		let diagnostics = compiler.check(vec![module]).await?;

		Ok(diagnostics)
	}
}

impl Server {
	pub(crate) async fn handle_check_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		dependency: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let Ok(dependency) = urlencoding::decode(dependency) else {
			return Ok(http::Response::builder().bad_request().empty().unwrap());
		};
		let Ok(dependency) = dependency.parse() else {
			return Ok(http::Response::builder().bad_request().empty().unwrap());
		};
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.check_package(&dependency, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
