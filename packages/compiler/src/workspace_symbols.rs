use {
	super::Compiler,
	futures::{TryStreamExt as _, stream::FuturesOrdered},
	lsp_types as lsp,
	tangram_client::prelude::*,
};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub query: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub symbols: Option<Vec<Symbol>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Symbol {
	pub name: String,
	pub kind: String,
	pub module: tg::module::Data,
	pub range: tg::Range,
	pub container_name: Option<String>,
	pub deprecated: Option<bool>,
}

impl Compiler {
	pub async fn workspace_symbols(&self, query: String) -> tg::Result<Option<Vec<Symbol>>> {
		// Create the request.
		let request = super::Request::WorkspaceSymbol(Request { query });

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::WorkspaceSymbol(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.symbols)
	}
}

impl Compiler {
	pub(crate) async fn handle_workspace_symbol_request(
		&self,
		params: lsp::WorkspaceSymbolParams,
	) -> tg::Result<Option<lsp::WorkspaceSymbolResponse>> {
		// Get the symbols.
		let symbols = self.workspace_symbols(params.query).await?;
		let Some(symbols) = symbols else {
			return Ok(None);
		};

		// Convert the symbols.
		let symbols = symbols
			.into_iter()
			.map(|symbol| {
				let compiler = self.clone();
				async move {
					let uri = compiler.lsp_uri_for_module(&symbol.module).await?;
					let tags = symbol
						.deprecated
						.unwrap_or(false)
						.then_some(vec![lsp::SymbolTag::DEPRECATED]);
					#[expect(deprecated)]
					let symbol = lsp::SymbolInformation {
						name: symbol.name,
						kind: symbol_kind_for_script_element_kind(&symbol.kind),
						tags,
						deprecated: None,
						location: lsp::Location {
							uri,
							range: symbol.range.into(),
						},
						container_name: symbol.container_name,
					};
					Ok::<_, tg::Error>(symbol)
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		let response = lsp::WorkspaceSymbolResponse::Flat(symbols);

		Ok(Some(response))
	}
}

fn symbol_kind_for_script_element_kind(kind: &str) -> lsp::SymbolKind {
	match kind {
		"class" | "local class" => lsp::SymbolKind::CLASS,
		"enum" => lsp::SymbolKind::ENUM,
		"enum member" => lsp::SymbolKind::ENUM_MEMBER,
		"interface" => lsp::SymbolKind::INTERFACE,
		"module" | "external module name" => lsp::SymbolKind::MODULE,
		"type" | "primitive type" | "type parameter" => lsp::SymbolKind::TYPE_PARAMETER,
		"const" => lsp::SymbolKind::CONSTANT,
		"property" | "accessor" | "getter" | "setter" => lsp::SymbolKind::PROPERTY,
		"method" => lsp::SymbolKind::METHOD,
		"function" | "local function" => lsp::SymbolKind::FUNCTION,
		"constructor" | "construct" => lsp::SymbolKind::CONSTRUCTOR,
		"directory" | "script" => lsp::SymbolKind::FILE,
		_ => lsp::SymbolKind::VARIABLE,
	}
}
