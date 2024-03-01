use super::Server;
use lsp_types as lsp;
use tangram_error::Result;

pub struct VirtualTextDocument;

impl lsp::request::Request for VirtualTextDocument {
	type Params = Params;
	type Result = Option<String>;
	const METHOD: &'static str = "tangram/virtualTextDocument";
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
	pub text_document: lsp::TextDocumentIdentifier,
}

impl Server {
	pub(super) async fn handle_virtual_text_document_request(
		&self,
		params: Params,
	) -> Result<Option<String>> {
		// Get the module.
		let module = self.module_for_url(&params.text_document.uri).await?;

		// Load the file.
		let text = self.load_module(&module).await?;

		Ok(Some(text))
	}
}
