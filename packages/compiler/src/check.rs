use {super::Compiler, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub modules: Vec<tg::module::Data>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub diagnostics: Vec<tg::diagnostic::Data>,
}

impl Compiler {
	/// Get all diagnostics for the provided modules.
	pub async fn check(&self, modules: Vec<tg::module::Data>) -> tg::Result<Vec<tg::Diagnostic>> {
		// Create the request.
		let request = super::Request::Check(Request { modules });

		// Perform the request.
		let response = self.request(request).await?.unwrap_check();

		// Convert diagnostics from data to the non-serializable form.
		let diagnostics = response
			.diagnostics
			.into_iter()
			.map(TryInto::try_into)
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(diagnostics)
	}
}
