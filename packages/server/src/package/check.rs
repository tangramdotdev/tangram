use crate::Server;
use tangram_client as tg;
use tangram_http::{
	outgoing::response::Ext as _,
	Incoming, Outgoing,
};

impl Server {
	pub async fn check_package(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Vec<tg::Diagnostic>> {
		// Get the package.
		let (package, lock) = tg::package::get_with_lock(self, dependency).await?;

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
		_request: http::Request<Incoming>,
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
		let output = handle.check_package(&dependency).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
