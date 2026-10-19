use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug)]
pub struct Output {
	pub help: Option<Help>,
}

#[derive(Debug)]
pub struct Help {
	pub active_parameter: Option<u32>,
	pub active_signature: Option<u32>,
	pub signatures: Vec<Signature>,
}

#[derive(Debug)]
pub struct Signature {
	pub active_parameter: Option<u32>,
	pub documentation: Option<String>,
	pub label: String,
	pub parameters: Vec<Parameter>,
}

#[derive(Debug)]
pub struct Parameter {
	pub documentation: Option<String>,
	pub label: String,
}

impl Compiler {
	pub async fn signature_help(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<Help>> {
		// Create the arg.
		let arg = Arg {
			module: module.clone(),
			position,
		};

		// Get the output.
		let output = self.backend(module.kind).signature_help(self, arg).await?;

		Ok(output.help)
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
