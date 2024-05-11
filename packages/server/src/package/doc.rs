use crate::Server;
use tangram_client as tg;
use tangram_http::{
	outgoing::{ResponseBuilderExt as _, ResponseExt as _},
	Incoming, Outgoing,
};

impl Server {
	pub async fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Option<serde_json::Value>> {
		// Get the package.
		let Some((package, lock)) = tg::package::try_get_with_lock(self, dependency).await? else {
			return Ok(None);
		};

		// Create the module.
		let module = tg::Module::with_package_and_lock(self, &package, &lock).await?;

		// Create the language server.
		let language_server = crate::language::Server::new(self, tokio::runtime::Handle::current());

		// Get the doc.
		let doc = language_server.doc(&module).await?;

		Ok(Some(doc))
	}
}

impl Server {
	pub(crate) async fn handle_get_package_doc_request<H>(
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

		// Get the doc.
		let Some(output) = handle.try_get_package_doc(&dependency).await? else {
			return Ok(http::Response::not_found());
		};

		// Create the response.
		let response = http::Response::builder().json(output).unwrap();

		Ok(response)
	}
}
