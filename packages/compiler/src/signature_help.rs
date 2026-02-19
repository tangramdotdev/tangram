use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub help: Option<Help>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Help {
	pub signatures: Vec<Signature>,
	pub active_signature: Option<u32>,
	pub active_parameter: Option<u32>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Signature {
	pub label: String,
	pub documentation: Option<String>,
	pub parameters: Vec<Parameter>,
	pub active_parameter: Option<u32>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Parameter {
	pub label: String,
	pub documentation: Option<String>,
}

impl Compiler {
	pub async fn signature_help(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<Help>> {
		// Create the request.
		let request = super::Request::SignatureHelp(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::SignatureHelp(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.help)
	}
}

impl Compiler {
	pub(super) async fn handle_signature_help_request(
		&self,
		params: lsp::SignatureHelpParams,
	) -> tg::Result<Option<lsp::SignatureHelp>> {
		// Get the module.
		let module = self
			.module_for_lsp_uri(&params.text_document_position_params.text_document.uri)
			.await?;

		// Get the position for the request.
		let position = params.text_document_position_params.position;

		// Get the signature help.
		let help = self.signature_help(&module, position.into()).await?;
		let Some(help) = help else {
			return Ok(None);
		};

		if help.signatures.is_empty() {
			return Ok(None);
		}

		// Convert the signatures.
		let signatures = help
			.signatures
			.into_iter()
			.map(|signature| {
				let documentation = signature.documentation.map(lsp::Documentation::String);
				let parameters = if signature.parameters.is_empty() {
					None
				} else {
					Some(
						signature
							.parameters
							.into_iter()
							.map(|parameter| lsp::ParameterInformation {
								label: lsp::ParameterLabel::Simple(parameter.label),
								documentation: parameter
									.documentation
									.map(lsp::Documentation::String),
							})
							.collect(),
					)
				};
				lsp::SignatureInformation {
					label: signature.label,
					documentation,
					parameters,
					active_parameter: signature.active_parameter,
				}
			})
			.collect();

		let help = lsp::SignatureHelp {
			signatures,
			active_signature: help.active_signature,
			active_parameter: help.active_parameter,
		};

		Ok(Some(help))
	}
}
