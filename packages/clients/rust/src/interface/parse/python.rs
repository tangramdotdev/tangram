use {
	super::{Error, Parser, TokenKind},
	crate::prelude::*,
	std::collections::BTreeMap,
};

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Script {
	#[serde(default)]
	tool: Tool,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Tool {
	#[serde(default)]
	tangram: Tangram,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Tangram {
	#[serde(default)]
	dependencies: BTreeMap<String, Dependency>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Dependency {
	#[serde(default)]
	kind: Option<String>,

	specifier: String,
}

/// Parse an interface file in Python, as a subset of Python type declarations with its imports in a script metadata block.
pub(super) fn parse(text: &str) -> Result<tg::Interface, Error> {
	// Get the imports from the script metadata block.
	let mut interface = tg::Interface {
		imports: imports(text)?,
		..Default::default()
	};

	// Parse the statements.
	let tokens = super::lex(text, tg::interface::Language::Python)?;
	let mut parser = Parser::new(tokens, text.len());
	while let Some(token) = parser.peek() {
		let start = token.range.start;
		if token.column != 0 {
			return parser.error("expected a statement without indentation");
		}
		if parser.is_keyword("import") || parser.is_keyword("from") {
			return parser.error("expected the imports in the script metadata block");
		}
		if parser.is_keyword("type")
			&& matches!(parser.peek_kind(1), Some(TokenKind::Identifier(_)))
		{
			parser.next();
			let name = parser.expect_identifier()?;
			parser.expect_punctuation("=")?;
			let type_ = type_(&mut parser)?;
			end(&parser)?;
			let range = start..parser.range().start;
			super::insert(&mut interface.definitions, name, type_, range, "type")?;
		} else if parser.eat_keyword("class") {
			let (name, type_) = class(&mut parser)?;
			let range = start..parser.range().start;
			super::insert(&mut interface.definitions, name, type_, range, "type")?;
		} else {
			let name = parser.expect_identifier()?;
			parser.expect_punctuation(":")?;
			let export = export(&mut parser)?;
			end(&parser)?;
			let range = start..parser.range().start;
			super::insert(&mut interface.exports, name, export, range, "export")?;
		}
	}

	// Check the references.
	parser.check_references(&interface)?;

	Ok(interface)
}

/// Get the imports from the `[tool.tangram.dependencies.<name>]` tables of the script metadata block.
fn imports(text: &str) -> Result<BTreeMap<String, tg::module::Data>, Error> {
	// Find the script metadata block.
	let mut offset = 0;
	let mut start = None;
	let mut lines = Vec::new();
	for line in text.split_inclusive('\n') {
		let begin = offset;
		offset += line.len();
		let line = line.trim_end_matches(['\r', '\n']);
		if start.is_none() {
			if line == "# /// script" {
				start = Some(begin);
			}
			continue;
		}
		if line == "# ///" {
			let range = start.unwrap()..offset;
			let toml = lines.join("\n");
			let script = toml::from_str::<Script>(&toml).map_err(|error| Error {
				message: error.message().to_owned(),
				range: range.clone(),
			})?;
			let mut imports = BTreeMap::new();
			for (name, dependency) in script.tool.tangram.dependencies {
				let module = super::import(&dependency.specifier, dependency.kind.as_deref())
					.map_err(|message| Error {
						message,
						range: range.clone(),
					})?;
				imports.insert(name, module);
			}
			return Ok(imports);
		}
		let Some(content) = line.strip_prefix('#') else {
			let message = "expected the script metadata block to be comments".to_owned();
			return Err(Error {
				message,
				range: begin..offset,
			});
		};
		lines.push(content.strip_prefix(' ').unwrap_or(content).to_owned());
	}
	if let Some(start) = start {
		let message = "the script metadata block is not closed".to_owned();
		return Err(Error {
			message,
			range: start..text.len(),
		});
	}
	Ok(BTreeMap::new())
}

/// Check that a statement ends at the end of its line.
fn end(parser: &Parser) -> Result<(), Error> {
	match parser.peek() {
		Some(token) if !token.line_start => parser.error("expected the end of the line"),
		_ => Ok(()),
	}
}

/// Parse a `TypedDict` class after `class`, whose fields are a record.
fn class(parser: &mut Parser) -> Result<(String, tg::interface::Type), Error> {
	let name = parser.expect_identifier()?;
	parser.expect_punctuation("(")?;
	parser.expect_keyword("TypedDict")?;
	if parser.eat_punctuation(",") && !parser.is_punctuation(")") {
		parser.expect_keyword("closed")?;
		parser.expect_punctuation("=")?;
		parser.expect_keyword("True")?;
		parser.eat_punctuation(",");
	}
	parser.expect_punctuation(")")?;
	parser.expect_punctuation(":")?;
	let mut fields = BTreeMap::new();
	let mut body = false;
	while let Some(token) = parser.peek() {
		if token.line_start && token.column == 0 {
			break;
		}
		if !token.line_start {
			return parser.error("expected the class's body on the lines after it");
		}
		body = true;
		if parser.eat_keyword("pass") {
			end(parser)?;
			continue;
		}
		let range = parser.range();
		let name = parser.expect_identifier()?;
		parser.expect_punctuation(":")?;
		let field = field(parser)?;
		end(parser)?;
		super::insert(&mut fields, name, field, range, "field")?;
	}
	if !body {
		return parser.error("expected the class's body");
	}
	Ok((name, tg::interface::Type::Record { fields }))
}

/// Parse the type of an export, with a docstring on the line after it, and `tg.NonValue["reason"]` for an export that is not a value.
fn export(parser: &mut Parser) -> Result<tg::interface::Export, Error> {
	let non_value = parser.is_keyword("tg")
		&& matches!(parser.peek_kind(2), Some(TokenKind::Identifier(name)) if name == "NonValue");
	let (error, type_) = if non_value {
		parser.next();
		parser.next();
		parser.next();
		parser.expect_punctuation("[")?;
		let reason = parser.expect_string()?;
		parser.expect_punctuation("]")?;
		(Some(reason), None)
	} else {
		(None, Some(type_(parser)?))
	};
	end(parser)?;
	let documentation = match parser.peek() {
		Some(token) if token.column == 0 && matches!(token.kind, TokenKind::String(_)) => {
			Some(parser.expect_string()?.trim().to_owned())
		},
		_ => None,
	};
	let export = tg::interface::Export {
		documentation,
		error,
		range: None,
		type_,
	};
	Ok(export)
}

/// Parse the type of a field, which is optional if it is `NotRequired[...]`.
fn field(parser: &mut Parser) -> Result<tg::interface::Field, Error> {
	let optional = parser.eat_keyword("NotRequired");
	let type_ = if optional {
		parser.expect_punctuation("[")?;
		let type_ = type_(parser)?;
		parser.expect_punctuation("]")?;
		type_
	} else {
		type_(parser)?
	};
	Ok(tg::interface::Field { optional, type_ })
}

fn type_(parser: &mut Parser) -> Result<tg::interface::Type, Error> {
	let mut types = vec![primary(parser)?];
	while parser.eat_punctuation("|") {
		types.push(primary(parser)?);
	}
	let type_ = if types.len() == 1 {
		types.pop().unwrap()
	} else {
		tg::interface::Type::Union { types }
	};
	Ok(type_)
}

fn primary(parser: &mut Parser) -> Result<tg::interface::Type, Error> {
	if parser.eat_punctuation("(") {
		let type_ = type_(parser)?;
		parser.expect_punctuation(")")?;
		return Ok(type_);
	}
	let Some(TokenKind::Identifier(name)) = parser.peek_kind(0).cloned() else {
		return parser.error("expected a type");
	};
	parser.next();
	let type_ = match name.as_str() {
		"Literal" => {
			parser.expect_punctuation("[")?;
			let mut types = Vec::new();
			loop {
				let value = parser.literal(["True", "False"])?;
				types.push(tg::interface::Type::Literal { value });
				if !parser.eat_punctuation(",") || parser.is_punctuation("]") {
					break;
				}
			}
			parser.expect_punctuation("]")?;
			if types.len() == 1 {
				types.pop().unwrap()
			} else {
				tg::interface::Type::Union { types }
			}
		},
		"Never" => tg::interface::Type::Union { types: Vec::new() },
		"None" => tg::interface::Type::Null,
		"TypedDict" => {
			parser.expect_punctuation("[")?;
			parser.expect_punctuation("{")?;
			let mut fields = BTreeMap::new();
			while !parser.eat_punctuation("}") {
				let range = parser.range();
				let name = parser.expect_string()?;
				parser.expect_punctuation(":")?;
				let field = field(parser)?;
				super::insert(&mut fields, name, field, range, "field")?;
				if !parser.eat_punctuation(",") {
					parser.expect_punctuation("}")?;
					break;
				}
			}
			parser.expect_punctuation("]")?;
			tg::interface::Type::Record { fields }
		},
		"bool" => tg::interface::Type::Boolean,
		"bytes" => tg::interface::Type::Bytes,
		"dict" => {
			parser.expect_punctuation("[")?;
			parser.expect_keyword("str")?;
			parser.expect_punctuation(",")?;
			let value = type_(parser)?;
			parser.expect_punctuation("]")?;
			tg::interface::Type::Map {
				value: Box::new(value),
			}
		},
		"float" | "int" => tg::interface::Type::Number,
		"list" => {
			parser.expect_punctuation("[")?;
			let item = type_(parser)?;
			parser.expect_punctuation("]")?;
			tg::interface::Type::Array {
				item: Box::new(item),
			}
		},
		"str" => tg::interface::Type::String,
		"tg" => tangram(parser)?,
		"tuple" => tuple(parser)?,
		_ => parser.reference(name)?,
	};
	Ok(type_)
}

/// Parse a Tangram type after `tg`, such as `tg.File` or `tg.Command[Args, Output]`.
fn tangram(parser: &mut Parser) -> Result<tg::interface::Type, Error> {
	parser.expect_punctuation(".")?;
	let range = parser.range();
	let name = parser.expect_identifier()?;
	let type_ = match name.as_str() {
		"Artifact" => tg::interface::Type::Artifact,
		"Blob" => tg::interface::Type::Blob,
		"Command" => {
			parser.expect_punctuation("[")?;
			let args = type_(parser)?;
			parser.expect_punctuation(",")?;
			let output = type_(parser)?;
			parser.expect_punctuation("]")?;
			tg::interface::Type::Command {
				args: Box::new(args),
				output: Box::new(output),
			}
		},
		"Directory" => tg::interface::Type::Directory,
		"Error" => tg::interface::Type::Error,
		"File" => tg::interface::Type::File,
		"Graph" => tg::interface::Type::Graph,
		"Module" => tg::interface::Type::Module,
		"Mutation" => tg::interface::Type::Mutation,
		"Object" => tg::interface::Type::Object,
		"Placeholder" => tg::interface::Type::Placeholder,
		"Symlink" => tg::interface::Type::Symlink,
		"Template" => tg::interface::Type::Template,
		"Value" => tg::interface::Type::Value,
		_ => {
			let message = format!("the type tg.{name} is not known");
			return Err(Error { message, range });
		},
	};
	Ok(type_)
}

/// Parse a tuple after `tuple`: `tuple[()]` is empty, `tuple[T, ...]` has only a rest, optional elements are `NotRequired[...]`, and a rest after elements is `*tuple[T, ...]`.
fn tuple(parser: &mut Parser) -> Result<tg::interface::Type, Error> {
	parser.expect_punctuation("[")?;
	if parser.eat_punctuation("(") {
		parser.expect_punctuation(")")?;
		parser.expect_punctuation("]")?;
		let type_ = tg::interface::Type::Tuple {
			elements: Vec::new(),
			rest: None,
		};
		return Ok(type_);
	}
	let mut elements = Vec::new();
	let mut rest = None;
	while !parser.eat_punctuation("]") {
		if rest.is_some() {
			return parser.error("expected the rest to be the last element of the tuple");
		}
		if parser.eat_punctuation("*") {
			parser.expect_keyword("tuple")?;
			parser.expect_punctuation("[")?;
			let item = type_(parser)?;
			parser.expect_punctuation(",")?;
			parser.expect_punctuation("...")?;
			parser.expect_punctuation("]")?;
			rest = Some(Box::new(item));
		} else if parser.is_punctuation("...") {
			let [element] = elements.as_slice() else {
				return parser.error("expected \"...\" only after the one element of a tuple");
			};
			let tg::interface::Element {
				optional: false,
				type_,
			} = element
			else {
				return parser.error("expected \"...\" after a required element");
			};
			parser.next();
			rest = Some(Box::new(type_.clone()));
			elements.clear();
		} else {
			let field = field(parser)?;
			let element = tg::interface::Element {
				optional: field.optional,
				type_: field.type_,
			};
			elements.push(element);
		}
		if !parser.eat_punctuation(",") {
			parser.expect_punctuation("]")?;
			break;
		}
	}
	Ok(tg::interface::Type::Tuple { elements, rest })
}
