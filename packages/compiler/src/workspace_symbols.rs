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
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub symbols: Option<Vec<Symbol>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Symbol {
	pub name: String,
	pub kind: String,
	pub module: tg::module::Data,
	pub range: tg::Range,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub container_name: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub deprecated: Option<bool>,
}

impl Compiler {
	pub async fn workspace_symbols(&self, query: String) -> tg::Result<Option<Vec<Symbol>>> {
		let mut symbols = Vec::new();
		if self
			.documents
			.iter()
			.any(|document| document.open && document.module.kind != tg::module::Kind::Py)
		{
			let request = super::Request::WorkspaceSymbol(Request {
				query: query.clone(),
			});
			let response = self.request(request).await?.unwrap_workspace_symbol();
			symbols.extend(
				response
					.symbols
					.into_iter()
					.flatten()
					.filter(|symbol| symbol.module.kind != tg::module::Kind::Py),
			);
		}

		#[cfg(feature = "py")]
		if self.py.is_started()
			|| self
				.documents
				.iter()
				.any(|document| document.module.kind == tg::module::Kind::Py)
		{
			let request = super::Request::WorkspaceSymbol(Request { query });
			let response = self.request_py(request).await?.unwrap_workspace_symbol();
			symbols.extend(response.symbols.into_iter().flatten());
		}
		Ok(Some(symbols))
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
