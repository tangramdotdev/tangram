use super::Compiler;
use itertools::Itertools as _;
use lsp_types as lsp;
use serde_with::{serde_as, DisplayFromStr};
use std::collections::BTreeMap;
use tangram_client as tg;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {}

#[serde_as]
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
	#[serde_as(as = "BTreeMap<DisplayFromStr, _>")]
	pub diagnostics: BTreeMap<tg::module::Reference, Vec<tg::Diagnostic>>,
}

impl Compiler {
	pub async fn get_diagnostics(
		&self,
	) -> tg::Result<BTreeMap<tg::module::Reference, Vec<tg::Diagnostic>>> {
		// Create the request.
		let request = super::Request::Diagnostics(Request {});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Diagnostics(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		// Get the result the response.
		let Response { diagnostics } = response;

		Ok(diagnostics)
	}

	pub async fn update_diagnostics(&self) -> tg::Result<()> {
		// Get the diagnostics.
		let mut diagnostics = self.diagnostics.write().await;

		// Clear the diagnostics.
		for (_, diagnostics) in diagnostics.iter_mut() {
			diagnostics.drain(..);
		}

		// Update the diagnostics.
		diagnostics.extend(self.get_diagnostics().await?);

		// Publish the diagnostics.
		for (module, diagnostics) in diagnostics.iter() {
			let version = self.get_module_version(module).await?;
			let diagnostics = diagnostics.iter().cloned().map_into().collect();
			let params = lsp::PublishDiagnosticsParams {
				uri: self.uri_for_module(module),
				diagnostics,
				version: Some(version),
			};
			self.send_notification::<lsp::notification::PublishDiagnostics>(params);
		}

		// Remove the modules with no diagnostics.
		diagnostics.retain(|_, diagnostics| !diagnostics.is_empty());

		Ok(())
	}
}
