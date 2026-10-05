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
		// Check the Python modules.
		let (python, modules): (Vec<_>, Vec<_>) = modules
			.into_iter()
			.partition(|module| module.kind == tg::module::Kind::Py);
		#[cfg(not(feature = "py"))]
		if !python.is_empty() {
			return Err(tg::error!("the py feature is not enabled"));
		}
		#[cfg(not(feature = "py"))]
		let mut diagnostics = Vec::new();
		#[cfg(feature = "py")]
		let mut diagnostics = if python.is_empty() {
			Vec::new()
		} else {
			self.check_py(python).await?
		};
		if modules.is_empty() {
			return Ok(diagnostics);
		}

		// Create the request.
		let request = super::Request::Check(Request { modules });

		// Perform the request.
		let response = self.request(request).await?.unwrap_check();

		// Convert diagnostics from data to the non-serializable form.
		let javascript = response
			.diagnostics
			.into_iter()
			.map(TryInto::try_into)
			.collect::<tg::Result<Vec<_>>>()?;

		diagnostics.extend(javascript);
		Ok(diagnostics)
	}
}
