use super::{error, send_notification, Sender, Server};
use lsp_types as lsp;
use std::collections::BTreeMap;
use tangram_client as tg;
use tangram_error::Result;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
	pub diagnostics: Vec<tg::Diagnostic>,
}

impl Server {
	pub async fn get_diagnostics(&self) -> Result<Vec<tg::Diagnostic>> {
		// Create the request.
		let request = super::Request::Diagnostics(Request {});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Diagnostics(response) = response else {
			return Err(error!("Unexpected response type."));
		};

		// Get the result the response.
		let Response { diagnostics } = response;

		Ok(diagnostics)
	}

	pub async fn update_diagnostics(&self, sender: &Sender) -> Result<()> {
		// Get the diagnostics.
		let diagnostics = self.get_diagnostics().await?;

		// Clear the existing diagnostics.
		let mut existing_diagnostics = self.inner.diagnostics.write().await;
		let mut diagnostics_for_module: BTreeMap<tg::Module, Vec<tg::Diagnostic>> =
			existing_diagnostics
				.drain(..)
				.filter_map(|diagnostic| {
					let module = diagnostic.location?.module;
					Some((module, Vec::new()))
				})
				.collect();

		// Add the new diagnostics.
		existing_diagnostics.extend(diagnostics.iter().cloned());
		for diagnostic in diagnostics {
			if let Some(location) = &diagnostic.location {
				diagnostics_for_module
					.entry(location.module.clone())
					.or_default()
					.push(diagnostic);
			}
		}

		// Publish the diagnostics.
		for (module, diagnostics) in diagnostics_for_module {
			let version = Some(self.get_module_version(&module).await?);
			let diagnostics = diagnostics.into_iter().map(Into::into).collect();
			send_notification::<lsp::notification::PublishDiagnostics>(
				sender,
				lsp::PublishDiagnosticsParams {
					uri: self.url_for_module(&module),
					diagnostics,
					version,
				},
			);
		}

		Ok(())
	}
}
