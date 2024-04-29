use crate::Server;
use tangram_client as tg;
use tangram_http::{outgoing::ResponseExt as _, Incoming, Outgoing};

impl Server {
	pub async fn check_package(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Vec<tg::Diagnostic>> {
		// Get the package.
		let (package, lock) = tg::package::get_with_lock(self, dependency).await?;

		// Create the root module.
		let path = tg::package::get_root_module_path(self, &package).await?;
		let package = package.id(self, None).await?;
		let lock = lock.id(self, None).await?;
		let module = tg::Module::Normal(tg::module::Normal {
			lock,
			package,
			path,
		});

		// Create the language server.
		let language_server = crate::language::Server::new(self, tokio::runtime::Handle::current());

		// Get the diagnostics.
		let diagnostics = language_server.check(vec![module]).await?;

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
			return Ok(http::Response::bad_request());
		};
		let Ok(dependency) = dependency.parse() else {
			return Ok(http::Response::bad_request());
		};

		// Check the package.
		let output = handle.check_package(&dependency).await?;

		// Create the response.
		let response = http::Response::builder()
			.body(Outgoing::json(output))
			.unwrap();

		Ok(response)
	}
}
