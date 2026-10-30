use {
	super::{Compiler, bindings_language},
	futures::{TryStreamExt as _, stream::FuturesOrdered},
	lsp_types as lsp,
	tangram_client::prelude::*,
};

#[derive(Debug)]
pub struct Arg {
	pub query: String,
}

#[derive(Debug)]
pub struct Output {
	pub symbols: Option<Vec<Symbol>>,
}

#[derive(Debug)]
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
		let mut symbols = Vec::new();
		if self
			.documents
			.iter()
			.any(|document| document.open && document.module.kind != tg::module::Kind::Python)
		{
			let arg = Arg {
				query: query.clone(),
			};
			let output = self
				.backend(tg::module::Kind::TypeScript)
				.workspace_symbol(self, arg)
				.await?;
			let javascript = self
				.map_workspace_symbols(
					output.symbols.unwrap_or_default(),
					tg::module::Kind::TypeScript,
				)
				.await?;
			symbols.extend(javascript);
		}

		#[cfg(feature = "ty")]
		if self.python.is_started()
			|| self
				.documents
				.iter()
				.any(|document| document.module.kind == tg::module::Kind::Python)
		{
			let arg = Arg { query };
			let output = self
				.backend(tg::module::Kind::Python)
				.workspace_symbol(self, arg)
				.await?;
			let python = self
				.map_workspace_symbols(output.symbols.unwrap_or_default(), tg::module::Kind::Python)
				.await?;
			symbols.extend(python);
		}
		Ok(Some(symbols))
	}

	/// Map the symbols in bindings to the homes of their modules' interfaces, dropping the symbols that have no place there and the symbols that another language service already reports there.
	async fn map_workspace_symbols(
		&self,
		symbols: Vec<Symbol>,
		kind: tg::module::Kind,
	) -> tg::Result<Vec<Symbol>> {
		let mut output = Vec::new();
		for mut symbol in symbols {
			if bindings_language(kind, symbol.module.kind).is_none() {
				output.push(symbol);
				continue;
			}
			if !symbol.module.kind.is_config() {
				continue;
			}
			let location = tg::module::data::Location {
				module: symbol.module.clone(),
				range: symbol.range,
			};
			let Some(location) = self.try_lsp_location(&location, kind).await? else {
				continue;
			};
			symbol.module = self.module_for_lsp_uri(&location.uri).await?;
			symbol.range = location.range.into();
			output.push(symbol);
		}
		Ok(output)
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
