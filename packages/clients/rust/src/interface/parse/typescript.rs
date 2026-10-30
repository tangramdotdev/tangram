use {
	super::{Error, Parser, TokenKind},
	crate::prelude::*,
	std::{collections::BTreeMap, ops::Range},
};

/// A declaration with `declare const` that `export { ... }` or `export default` exports.
struct Local {
	export: tg::interface::Export,
	range: Range<usize>,
	used: bool,
}

/// Parse an interface file in TypeScript, as a subset of TypeScript declarations.
pub(super) fn parse(text: &str) -> Result<tg::Interface, Error> {
	let tokens = super::lex(text, tg::interface::Language::TypeScript)?;
	let mut parser = Parser::new(tokens, text);
	let mut interface = tg::Interface::default();
	let mut locals = BTreeMap::<String, Local>::new();
	let mut exports = Vec::new();
	while parser.peek().is_some() {
		// Get the documentation of the next declaration.
		let documentation = match parser.peek_kind(0) {
			Some(TokenKind::Documentation(documentation)) => {
				let documentation = documentation.clone();
				parser.next();
				Some(documentation)
			},
			_ => None,
		};
		if parser.peek().is_none() {
			break;
		}
		let start = parser.range().start;

		// Parse the declaration.
		if parser.eat_keyword("import") {
			let (alias, module) = import(&mut parser)?;
			super::insert(
				&mut interface.imports,
				alias,
				module,
				start..parser.range().start,
				"import",
			)?;
		} else if parser.eat_keyword("export") {
			if parser.eat_keyword("declare") {
				parser.expect_keyword("const")?;
				let name_range = parser.position_range();
				let name = parser.expect_identifier()?;
				parser.expect_punctuation(":")?;
				let export = export(&mut parser, documentation, name_range)?;
				let range = start..parser.range().start;
				super::insert(&mut interface.exports, name, export, range, "export")?;
			} else if parser.eat_punctuation("{") {
				while !parser.eat_punctuation("}") {
					let range = parser.range();
					let local = parser.expect_identifier()?;
					let name = if parser.eat_keyword("as") {
						match parser.peek_kind(0) {
							Some(TokenKind::String(_)) => parser.expect_string()?,
							_ => parser.expect_identifier()?,
						}
					} else {
						local.clone()
					};
					exports.push((local, name, range));
					if !parser.eat_punctuation(",") {
						parser.expect_punctuation("}")?;
						break;
					}
				}
			} else if parser.eat_keyword("default") {
				let range = parser.range();
				let local = parser.expect_identifier()?;
				exports.push((local, "default".to_owned(), range));
			} else if parser.eat_keyword("type") {
				let (name, type_) = definition(&mut parser)?;
				let range = start..parser.range().start;
				super::insert(&mut interface.definitions, name, type_, range, "type")?;
			} else {
				return parser.error("expected a declaration, a type, or a list of exports");
			}
		} else if parser.eat_keyword("declare") {
			parser.expect_keyword("const")?;
			let name_range = parser.position_range();
			let name = parser.expect_identifier()?;
			parser.expect_punctuation(":")?;
			let export = export(&mut parser, documentation, name_range)?;
			let range = start..parser.range().start;
			let local = Local {
				export,
				range: range.clone(),
				used: false,
			};
			super::insert(&mut locals, name, local, range, "declaration")?;
		} else if parser.eat_keyword("type") {
			let (name, type_) = definition(&mut parser)?;
			let range = start..parser.range().start;
			super::insert(&mut interface.definitions, name, type_, range, "type")?;
		} else {
			return parser.error("expected an import, an export, or a type");
		}
		parser.eat_punctuation(";");
	}

	// Export the declarations that the export lists name.
	for (local, name, range) in exports {
		let Some(declaration) = locals.get_mut(&local) else {
			let message = format!("the declaration {local:?} is not declared");
			return Err(Error { message, range });
		};
		declaration.used = true;
		let export = declaration.export.clone();
		super::insert(&mut interface.exports, name, export, range, "export")?;
	}
	if let Some((name, local)) = locals.iter().find(|(_, local)| !local.used) {
		let message = format!("the declaration {name:?} is not exported");
		let range = local.range.clone();
		return Err(Error { message, range });
	}

	// Check the references.
	parser.check_references(&interface)?;

	Ok(interface)
}

/// Parse an import, `import type * as name from "specifier"`, with an optional `with { kind: "..." }`.
fn import(parser: &mut Parser) -> Result<(String, tg::module::Data), Error> {
	parser.expect_keyword("type")?;
	parser.expect_punctuation("*")?;
	parser.expect_keyword("as")?;
	let alias = parser.expect_identifier()?;
	parser.expect_keyword("from")?;
	let range = parser.range();
	let specifier = parser.expect_string()?;
	let mut kind = None;
	if parser.eat_keyword("with") {
		parser.expect_punctuation("{")?;
		parser.expect_keyword("kind")?;
		parser.expect_punctuation(":")?;
		kind = Some(parser.expect_string()?);
		parser.eat_punctuation(",");
		parser.expect_punctuation("}")?;
	}
	let module =
		super::import(&specifier, kind.as_deref()).map_err(|message| Error { message, range })?;
	Ok((alias, module))
}

/// Parse the name and type of a definition, `Name = Type`.
fn definition(parser: &mut Parser) -> Result<(String, tg::interface::Type), Error> {
	let name = parser.expect_identifier()?;
	parser.expect_punctuation("=")?;
	let type_ = type_(parser)?;
	Ok((name, type_))
}

/// Parse the type of an export, which is a type, or `tg.NonValue<"reason">` for an export that is not a value, with the range of the name of its declaration.
fn export(
	parser: &mut Parser,
	documentation: Option<String>,
	range: Option<tg::Range>,
) -> Result<tg::interface::Export, Error> {
	let non_value = matches!(parser.peek_kind(0), Some(TokenKind::Identifier(name)) if name == "tg")
		&& matches!(parser.peek_kind(2), Some(TokenKind::Identifier(name)) if name == "NonValue");
	let (error, type_) = if non_value {
		parser.next();
		parser.next();
		parser.next();
		parser.expect_punctuation("<")?;
		let reason = parser.expect_string()?;
		parser.expect_punctuation(">")?;
		(Some(reason), None)
	} else {
		(None, Some(type_(parser)?))
	};
	let export = tg::interface::Export {
		documentation,
		error,
		range,
		type_,
	};
	Ok(export)
}

fn type_(parser: &mut Parser) -> Result<tg::interface::Type, Error> {
	parser.eat_punctuation("|");
	let mut types = vec![postfix(parser)?];
	while parser.eat_punctuation("|") {
		types.push(postfix(parser)?);
	}
	let type_ = if types.len() == 1 {
		types.pop().unwrap()
	} else {
		tg::interface::Type::Union { types }
	};
	Ok(type_)
}

fn postfix(parser: &mut Parser) -> Result<tg::interface::Type, Error> {
	let mut type_ = primary(parser)?;
	while parser.is_punctuation("[")
		&& matches!(parser.peek_kind(1), Some(TokenKind::Punctuation("]")))
	{
		parser.next();
		parser.next();
		type_ = tg::interface::Type::Array {
			item: Box::new(type_),
		};
	}
	Ok(type_)
}

fn primary(parser: &mut Parser) -> Result<tg::interface::Type, Error> {
	let type_ = match parser.peek_kind(0).cloned() {
		Some(TokenKind::Punctuation("(")) => {
			parser.next();
			let type_ = type_(parser)?;
			parser.expect_punctuation(")")?;
			type_
		},
		Some(TokenKind::Punctuation("[")) => {
			parser.next();
			tuple(parser)?
		},
		Some(TokenKind::Punctuation("{")) => {
			parser.next();
			object(parser)?
		},
		Some(TokenKind::Number(_) | TokenKind::String(_) | TokenKind::Punctuation("-")) => {
			let value = parser.literal(["true", "false"])?;
			tg::interface::Type::Literal { value }
		},
		Some(TokenKind::Identifier(name)) => {
			if name == "true" || name == "false" {
				let value = parser.literal(["true", "false"])?;
				return Ok(tg::interface::Type::Literal { value });
			}
			parser.next();
			match name.as_str() {
				"Array" => {
					parser.expect_punctuation("<")?;
					let item = type_(parser)?;
					parser.expect_punctuation(">")?;
					tg::interface::Type::Array {
						item: Box::new(item),
					}
				},
				"Uint8Array" => tg::interface::Type::Bytes,
				"boolean" => tg::interface::Type::Boolean,
				"never" => tg::interface::Type::Union { types: Vec::new() },
				"null" => tg::interface::Type::Null,
				"number" => tg::interface::Type::Number,
				"string" => tg::interface::Type::String,
				"tg" => tangram(parser)?,
				_ => parser.reference(name)?,
			}
		},
		_ => return parser.error("expected a type"),
	};
	Ok(type_)
}

/// Parse a Tangram type after `tg`, such as `tg.File` or `tg.Command<Args, Output>`.
fn tangram(parser: &mut Parser) -> Result<tg::interface::Type, Error> {
	parser.expect_punctuation(".")?;
	let range = parser.range();
	let name = parser.expect_identifier()?;
	let type_ = match name.as_str() {
		"Artifact" => tg::interface::Type::Artifact,
		"Blob" => tg::interface::Type::Blob,
		"Command" => {
			parser.expect_punctuation("<")?;
			let args = type_(parser)?;
			parser.expect_punctuation(",")?;
			let output = type_(parser)?;
			parser.expect_punctuation(">")?;
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

/// Parse a tuple after `[`, with optional elements marked by `?` and a rest marked by `...`.
fn tuple(parser: &mut Parser) -> Result<tg::interface::Type, Error> {
	let mut elements = Vec::new();
	let mut rest = None;
	while !parser.eat_punctuation("]") {
		if rest.is_some() {
			return parser.error("expected the rest to be the last element of the tuple");
		}
		if parser.eat_punctuation("...") {
			let range = parser.range();
			let tg::interface::Type::Array { item } = type_(parser)? else {
				let message = "expected an array type after \"...\"".to_owned();
				return Err(Error { message, range });
			};
			rest = Some(item);
		} else {
			let type_ = type_(parser)?;
			let optional = parser.eat_punctuation("?");
			elements.push(tg::interface::Element { optional, type_ });
		}
		if !parser.eat_punctuation(",") {
			parser.expect_punctuation("]")?;
			break;
		}
	}
	Ok(tg::interface::Type::Tuple { elements, rest })
}

/// Parse an object type after `{`, which is a record of fields or a map with an index signature.
fn object(parser: &mut Parser) -> Result<tg::interface::Type, Error> {
	let mut fields = BTreeMap::new();
	let mut map = None;
	while !parser.eat_punctuation("}") {
		let range = parser.range();
		if parser.eat_punctuation("[") {
			parser.expect_identifier()?;
			parser.expect_punctuation(":")?;
			parser.expect_keyword("string")?;
			parser.expect_punctuation("]")?;
			parser.expect_punctuation(":")?;
			let value = type_(parser)?;
			if map.is_some() || !fields.is_empty() {
				let message =
					"expected an object type with either fields or one index signature".to_owned();
				return Err(Error { message, range });
			}
			map = Some(value);
		} else {
			let name = match parser.peek_kind(0) {
				Some(TokenKind::String(_)) => parser.expect_string()?,
				_ => parser.expect_identifier()?,
			};
			let optional = parser.eat_punctuation("?");
			parser.expect_punctuation(":")?;
			let type_ = type_(parser)?;
			if map.is_some() {
				let message =
					"expected an object type with either fields or one index signature".to_owned();
				return Err(Error { message, range });
			}
			let field = tg::interface::Field { optional, type_ };
			super::insert(&mut fields, name, field, range, "field")?;
		}
		if !(parser.eat_punctuation(";") || parser.eat_punctuation(",")) {
			parser.expect_punctuation("}")?;
			break;
		}
	}
	let type_ = match map {
		Some(value) => tg::interface::Type::Map {
			value: Box::new(value),
		},
		None => tg::interface::Type::Record { fields },
	};
	Ok(type_)
}
