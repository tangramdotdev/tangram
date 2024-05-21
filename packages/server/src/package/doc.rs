use crate::Server;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::doc::Arg,
	) -> tg::Result<Option<serde_json::Value>> {
		// Get the package.
		let Some((package, lock)) =
			tg::package::try_get_with_lock(self, dependency, arg.locked).await?
		else {
			return Ok(None);
		};

		// Create the module.
		let module = tg::Module::with_package_and_lock(self, &package, &lock).await?;

		// Create the compiler.
		let compiler = crate::compiler::Compiler::new(self, tokio::runtime::Handle::current());

		// Get the doc.
		let doc = compiler.doc(&module).await?;

		Ok(Some(doc))
	}
}

impl Server {
	pub(crate) async fn handle_get_package_doc_request<H>(
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

		// Get the doc.
		let Some(output) = handle.try_get_package_doc(&dependency, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Create the response.
		let response = http::Response::builder().json(output).unwrap();

		Ok(response)
	}
}
