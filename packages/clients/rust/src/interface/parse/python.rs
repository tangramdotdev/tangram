use {
	super::{Error, Parser, TokenKind},
	crate::prelude::*,
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::Range,
	},
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

/// Parse an interface file in Python, as a subset of Python type declarations with its imports in a script metadata block, its re-exported namespaces as `import dependency` or `import dependency as name`, and its re-exported exports as `from dependency import export` or `from dependency import export as name`.
pub(super) fn parse(text: &str) -> Result<tg::Interface, Error> {
	// Get the dependencies from the script metadata block.
	let (dependencies, block) = dependencies(text)?;
	let mut interface = tg::Interface::default();

	// Parse the statements.
	let tokens = super::lex(text, tg::interface::Language::Python)?;
	let mut parser = Parser::new(tokens, text);
	let mut reexports = BTreeSet::new();
	while let Some(token) = parser.peek() {
		let start = token.range.start;
		if token.column != 0 {
			return parser.error("expected a statement without indentation");
		}
		if parser.eat_keyword("import") {
			let (name, dependency, export) = namespace(&mut parser, &dependencies, &block)?;
			let range = start..parser.range().start;
			super::insert(&mut interface.exports, name, export, range, "export")?;
			reexports.insert(dependency);
			continue;
		}
		if parser.eat_keyword("from") {
			let (dependency, exports) = from(&mut parser, &dependencies, &block)?;
			for (name, export, range) in exports {
				super::insert(&mut interface.exports, name, export, range, "export")?;
			}
			reexports.insert(dependency);
			continue;
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
			let name_range = parser.position_range();
			let name = parser.expect_identifier()?;
			if parser.eat_punctuation("=") {
				let type_ = typed_dict(&mut parser, &name)?;
				end(&parser)?;
				let range = start..parser.range().start;
				super::insert(&mut interface.definitions, name, type_, range, "type")?;
				continue;
			}
			parser.expect_punctuation(":")?;
			let export = export(&mut parser, name_range)?;
			end(&parser)?;
			let range = start..parser.range().start;
			super::insert(&mut interface.exports, name, export, range, "export")?;
		}
	}

	// Import the dependencies that types refer to or that no re-export imports.
	let references = parser
		.references
		.iter()
		.filter_map(|(import, _, _)| import.clone())
		.collect::<BTreeSet<_>>();
	for (name, dependency) in dependencies {
		if reexports.contains(&name) && !references.contains(&name) {
			continue;
		}
		let module = super::import(&dependency.specifier, dependency.kind.as_deref()).map_err(
			|message| Error {
				message,
				range: block.clone(),
			},
		)?;
		interface.imports.insert(name, module);
	}

	// Check the references.
	parser.check_references(&interface)?;

	Ok(interface)
}

/// Get the dependencies from the `[tool.tangram.dependencies.<name>]` tables of the script metadata block, with the range of the block.
fn dependencies(text: &str) -> Result<(BTreeMap<String, Dependency>, Range<usize>), Error> {
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
			let output = (script.tool.tangram.dependencies, range);
			return Ok(output);
		}
		let Some(content) = line.strip_prefix('#') else {
			let message = "expected the script metadata block to be comments".to_owned();
			return Err(Error {
				message,
				range: begin..offset,
			});
		};
		lines.push(content.trim_prefix(' ').to_owned());
	}
	if let Some(start) = start {
		let message = "the script metadata block is not closed".to_owned();
		return Err(Error {
			message,
			range: start..text.len(),
		});
	}
	Ok((BTreeMap::new(), 0..0))
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

/// Parse a call to `TypedDict` after `=`, whose fields are a record.
fn typed_dict(parser: &mut Parser, name: &str) -> Result<tg::interface::Type, Error> {
	parser.expect_keyword("TypedDict")?;
	parser.expect_punctuation("(")?;
	if parser.expect_string()? != name {
		return parser.error("expected the typed dict's name to match its declaration");
	}
	parser.expect_punctuation(",")?;
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
	parser.expect_punctuation(",")?;
	parser.expect_keyword("closed")?;
	parser.expect_punctuation("=")?;
	parser.expect_keyword("True")?;
	parser.eat_punctuation(",");
	parser.expect_punctuation(")")?;
	Ok(tg::interface::Type::Record { fields })
}

/// Parse the type of an export, with a docstring on the line after it, and `tg.NonValue["reason"]` for an export that is not a value, with the range of its name.
fn export(parser: &mut Parser, range: Option<tg::Range>) -> Result<tg::interface::Export, Error> {
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
	let documentation = documentation(parser)?;
	let export = tg::interface::Export {
		documentation,
		error,
		range,
		reexport: None,
		type_,
	};
	Ok(export)
}

/// Parse a re-exported namespace after `import`, `dependency` or `dependency as name`, with a docstring on the line after it, returning its name and its dependency.
fn namespace(
	parser: &mut Parser,
	dependencies: &BTreeMap<String, Dependency>,
	block: &Range<usize>,
) -> Result<(String, String, tg::interface::Export), Error> {
	let mut name_range = parser.position_range();
	let (dependency, import) = self::dependency(parser, dependencies, block)?;
	let name = if parser.eat_keyword("as") {
		name_range = parser.position_range();
		parser.expect_identifier()?
	} else {
		dependency.clone()
	};
	end(parser)?;
	let documentation = documentation(parser)?;
	let reexport = tg::interface::Reexport {
		export: None,
		import,
	};
	let export = tg::interface::Export {
		documentation,
		error: None,
		range: name_range,
		reexport: Some(reexport),
		type_: None,
	};
	Ok((name, dependency, export))
}

/// Parse re-exported exports after `from`, `dependency import export, other as name`, with a docstring on the line after them, returning their dependency and their names, exports, and ranges.
fn from(
	parser: &mut Parser,
	dependencies: &BTreeMap<String, Dependency>,
	block: &Range<usize>,
) -> Result<(String, Vec<(String, tg::interface::Export, Range<usize>)>), Error> {
	let (dependency, import) = self::dependency(parser, dependencies, block)?;
	parser.expect_keyword("import")?;
	let mut names = Vec::new();
	loop {
		let range = parser.range();
		let mut name_range = parser.position_range();
		let member = parser.expect_identifier()?;
		let name = if parser.eat_keyword("as") {
			name_range = parser.position_range();
			parser.expect_identifier()?
		} else {
			member.clone()
		};
		names.push((member, name, name_range, range));
		if !parser.eat_punctuation(",") {
			break;
		}
	}
	end(parser)?;
	let documentation = documentation(parser)?;
	let exports = names
		.into_iter()
		.map(|(member, name, name_range, range)| {
			let reexport = tg::interface::Reexport {
				export: Some(member),
				import: import.clone(),
			};
			let export = tg::interface::Export {
				documentation: documentation.clone(),
				error: None,
				range: name_range,
				reexport: Some(reexport),
				type_: None,
			};
			(name, export, range)
		})
		.collect();
	Ok((dependency, exports))
}

/// Parse the name of a dependency that a re-export imports, which the script metadata block must declare, returning the name and the dependency's import.
fn dependency(
	parser: &mut Parser,
	dependencies: &BTreeMap<String, Dependency>,
	block: &Range<usize>,
) -> Result<(String, tg::module::Import), Error> {
	let range = parser.range();
	let dependency = parser.expect_identifier()?;
	let Some(declaration) = dependencies.get(&dependency) else {
		let message = format!("the dependency {dependency:?} is not declared");
		return Err(Error { message, range });
	};
	let import = super::reexport(&declaration.specifier, declaration.kind.as_deref()).map_err(
		|message| Error {
			message,
			range: block.clone(),
		},
	)?;
	Ok((dependency, import))
}

/// Parse the docstring on the line after a statement, if there is one.
fn documentation(parser: &mut Parser) -> Result<Option<String>, Error> {
	let documentation = match parser.peek() {
		Some(token) if token.column == 0 && matches!(token.kind, TokenKind::String(_)) => {
			Some(parser.expect_string()?.trim().to_owned())
		},
		_ => None,
	};
	Ok(documentation)
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
