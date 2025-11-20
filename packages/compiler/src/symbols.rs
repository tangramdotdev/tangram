use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub module: tg::module::Data,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub symbols: Option<Vec<Symbol>>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Symbol {
	pub name: String,
	pub detail: Option<String>,
	pub kind: Kind,
	pub tags: Vec<Tag>,
	pub range: tg::Range,
	pub selection: tg::Range,
	pub children: Option<Vec<Self>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
	Array,
	Boolean,
	Class,
	Constant,
	Constructor,
	Enum,
	EnumMember,
	Event,
	Field,
	File,
	Function,
	Interface,
	Key,
	Method,
	Module,
	Namespace,
	Null,
	Number,
	Object,
	Operator,
	Package,
	Property,
	String,
	TypeParameter,
	Variable,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tag {
	Deprecated,
}

impl Compiler {
	pub async fn symbols(&self, module: &tg::module::Data) -> tg::Result<Option<Vec<Symbol>>> {
		// Create the request.
		let request = super::Request::Symbols(Request {
			module: module.clone(),
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Symbols(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.symbols)
	}
}

fn collect_symbol_tree(symbol: Symbol) -> lsp::DocumentSymbol {
	let Symbol {
		name,
		detail,
		kind,
		tags,
		range,
		selection: selection_range,
		children,
	} = symbol;

	let kind = match kind {
		Kind::Array => lsp::SymbolKind::ARRAY,
		Kind::Boolean => lsp::SymbolKind::BOOLEAN,
		Kind::Class => lsp::SymbolKind::CLASS,
		Kind::Constant => lsp::SymbolKind::CONSTANT,
		Kind::Constructor => lsp::SymbolKind::CONSTRUCTOR,
		Kind::Enum => lsp::SymbolKind::ENUM,
		Kind::EnumMember => lsp::SymbolKind::ENUM_MEMBER,
		Kind::Event => lsp::SymbolKind::EVENT,
		Kind::Field => lsp::SymbolKind::FIELD,
		Kind::File => lsp::SymbolKind::FILE,
		Kind::Function => lsp::SymbolKind::FUNCTION,
		Kind::Interface => lsp::SymbolKind::INTERFACE,
		Kind::Key => lsp::SymbolKind::KEY,
		Kind::Method => lsp::SymbolKind::METHOD,
		Kind::Module => lsp::SymbolKind::MODULE,
		Kind::Namespace => lsp::SymbolKind::NAMESPACE,
		Kind::Null => lsp::SymbolKind::NULL,
		Kind::Number => lsp::SymbolKind::NUMBER,
		Kind::Object => lsp::SymbolKind::OBJECT,
		Kind::Operator => lsp::SymbolKind::OPERATOR,
		Kind::Package => lsp::SymbolKind::PACKAGE,
		Kind::Property => lsp::SymbolKind::PROPERTY,
		Kind::String => lsp::SymbolKind::STRING,
		Kind::TypeParameter => lsp::SymbolKind::TYPE_PARAMETER,
		Kind::Variable => lsp::SymbolKind::VARIABLE,
	};

	let tags = tags
		.into_iter()
		.map(|tag| match tag {
			Tag::Deprecated => lsp::SymbolTag::DEPRECATED,
		})
		.collect();

	let children = children.map(|children| children.into_iter().map(collect_symbol_tree).collect());

	let range = lsp::Range {
		start: lsp::Position {
			line: range.start.line,
			character: range.end.character,
		},
		end: lsp::Position {
			line: range.end.line,
			character: range.end.character,
		},
	};

	let selection_range = lsp::Range {
		start: lsp::Position {
			line: selection_range.start.line,
			character: selection_range.end.character,
		},
		end: lsp::Position {
			line: selection_range.end.line,
			character: selection_range.end.character,
		},
	};

	#[expect(deprecated)]
	lsp::DocumentSymbol {
		name,
		detail,
		kind,
		tags: Some(tags),
		range,
		selection_range,
		children,
		deprecated: None,
	}
}

impl Compiler {
	pub(super) async fn handle_document_symbol_request(
		&self,
		params: lsp::DocumentSymbolParams,
	) -> tg::Result<Option<lsp::DocumentSymbolResponse>> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Get the document symbols.
		let symbols = self.symbols(&module).await?;
		let Some(symbols) = symbols else {
			return Ok(None);
		};

		// Convert the symbols.
		let symbols = symbols.into_iter().map(collect_symbol_tree).collect();

		Ok(Some(lsp::DocumentSymbolResponse::Nested(symbols)))
	}
}
