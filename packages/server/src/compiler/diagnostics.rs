use super::Compiler;
use futures::{TryStreamExt as _, stream};
use itertools::Itertools as _;
use lsp_types as lsp;
use std::collections::BTreeMap;
use tangram_client as tg;
use tokio_stream::StreamExt as _;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
	pub diagnostics: Vec<tg::Diagnostic>,
}

impl Compiler {
	pub async fn get_diagnostics(&self) -> tg::Result<Vec<tg::Diagnostic>> {
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
		diagnostics.extend(
			stream::iter(self.get_diagnostics().await?)
				.map(Ok::<_, tg::Error>)
				.try_fold(BTreeMap::default(), {
					move |mut map: BTreeMap<lsp::Uri, Vec<tg::Diagnostic>>, diagnostic| {
						let compiler = self.clone();
						async move {
							let Some(location) = diagnostic.location.clone() else {
								return Ok(map);
							};
							let uri = compiler.lsp_uri_for_module(&location.module).await?;
							map.entry(uri).or_default().push(diagnostic.clone());
							Ok(map)
						}
					}
				})
				.await?,
		);

		// Publish the diagnostics.
		for (uri, diagnostics) in diagnostics.iter() {
			let module = self.module_for_lsp_uri(uri).await?;
			let version = self.get_module_version(&module).await?;
			let diagnostics = diagnostics.iter().cloned().map_into().collect();
			let params = lsp::PublishDiagnosticsParams {
				uri: uri.clone(),
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
