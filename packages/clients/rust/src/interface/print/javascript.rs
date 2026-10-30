use {crate::prelude::*, std::fmt::Write as _};

/// The words that cannot be the names of declarations in TypeScript.
const RESERVED: &[&str] = &[
	"any",
	"as",
	"boolean",
	"break",
	"case",
	"catch",
	"class",
	"const",
	"continue",
	"debugger",
	"declare",
	"default",
	"delete",
	"do",
	"else",
	"enum",
	"export",
	"extends",
	"false",
	"finally",
	"for",
	"from",
	"function",
	"if",
	"implements",
	"import",
	"in",
	"instanceof",
	"interface",
	"let",
	"never",
	"new",
	"null",
	"number",
	"object",
	"package",
	"private",
	"protected",
	"public",
	"return",
	"static",
	"string",
	"super",
	"switch",
	"symbol",
	"this",
	"throw",
	"true",
	"try",
	"type",
	"typeof",
	"undefined",
	"unknown",
	"var",
	"void",
	"while",
	"with",
	"yield",
];

/// Print an interface in the JavaScript syntax.
pub(super) fn print(interface: &tg::Interface) -> tg::Result<String> {
	let mut output = "// @tangram interface javascript\n".to_owned();

	// Print the imports.
	if !interface.imports.is_empty() {
		output.push('\n');
	}
	for (alias, module) in &interface.imports {
		if !super::is_identifier(alias, RESERVED) {
			return Err(tg::error!(%alias, "the name of the import is not an identifier"));
		}
		let (specifier, kind) = super::import(module)?;
		let specifier = super::string(&specifier);
		write!(output, "import type * as {alias} from {specifier}").unwrap();
		if let Some(kind) = kind {
			write!(output, " with {{ kind: {} }}", super::string(&kind)).unwrap();
		}
		output.push_str(";\n");
	}

	// Print the definitions.
	if !interface.definitions.is_empty() {
		output.push('\n');
	}
	for (name, type_) in &interface.definitions {
		if !super::is_identifier(name, RESERVED) {
			return Err(tg::error!(%name, "the name of the type is not an identifier"));
		}
		writeln!(output, "export type {name} = {};", self::type_(type_)?).unwrap();
	}

	// Print the exports, declaring the exports whose names are not identifiers with local names, and exporting them by name.
	let mut renames = Vec::new();
	for (name, export) in &interface.exports {
		output.push('\n');
		if let Some(documentation) = &export.documentation {
			output.push_str(&self::documentation(documentation));
		}
		let type_ = match (&export.error, &export.type_) {
			(Some(reason), _) => format!("tg.NonValue<{}>", super::string(reason)),
			(None, Some(type_)) => self::type_(type_)?,
			(None, None) => "tg.Value".to_owned(),
		};
		if super::is_identifier(name, RESERVED) {
			writeln!(output, "export declare const {name}: {type_};").unwrap();
		} else {
			let local = format!("_export_{}", renames.len());
			writeln!(output, "declare const {local}: {type_};").unwrap();
			let name = if name == "default" {
				name.clone()
			} else {
				super::string(name)
			};
			renames.push(format!("{local} as {name}"));
		}
	}
	if !renames.is_empty() {
		writeln!(output, "\nexport {{ {} }};", renames.join(", ")).unwrap();
	}

	Ok(output)
}

fn documentation(documentation: &str) -> String {
	let documentation = documentation.replace("*/", "*\\/");
	if documentation.contains('\n') {
		let lines = documentation
			.lines()
			.map(|line| {
				if line.is_empty() {
					" *".to_owned()
				} else {
					format!(" * {line}")
				}
			})
			.collect::<Vec<_>>()
			.join("\n");
		format!("/**\n{lines}\n */\n")
	} else {
		format!("/** {documentation} */\n")
	}
}

fn type_(type_: &tg::interface::Type) -> tg::Result<String> {
	let output = match type_ {
		tg::interface::Type::Array { item } => format!("Array<{}>", self::type_(item)?),
		tg::interface::Type::Artifact => "tg.Artifact".to_owned(),
		tg::interface::Type::Blob => "tg.Blob".to_owned(),
		tg::interface::Type::Boolean => "boolean".to_owned(),
		tg::interface::Type::Bytes => "Uint8Array".to_owned(),
		tg::interface::Type::Command { args, output } => {
			format!(
				"tg.Command<{}, {}>",
				self::type_(args)?,
				self::type_(output)?
			)
		},
		tg::interface::Type::Directory => "tg.Directory".to_owned(),
		tg::interface::Type::Error => "tg.Error".to_owned(),
		tg::interface::Type::File => "tg.File".to_owned(),
		tg::interface::Type::Graph => "tg.Graph".to_owned(),
		tg::interface::Type::Literal { value } => match value {
			tg::interface::Literal::Boolean(value) => value.to_string(),
			tg::interface::Literal::Number(value) => super::number(*value)?,
			tg::interface::Literal::String(value) => super::string(value),
		},
		tg::interface::Type::Map { value } => {
			format!("{{ [key: string]: {} }}", self::type_(value)?)
		},
		tg::interface::Type::Module => "tg.Module".to_owned(),
		tg::interface::Type::Mutation => "tg.Mutation".to_owned(),
		tg::interface::Type::Null => "null".to_owned(),
		tg::interface::Type::Number => "number".to_owned(),
		tg::interface::Type::Object => "tg.Object".to_owned(),
		tg::interface::Type::Placeholder => "tg.Placeholder".to_owned(),
		tg::interface::Type::Record { fields } => {
			if fields.is_empty() {
				return Ok("{}".to_owned());
			}
			let fields = fields
				.iter()
				.map(|(name, field)| {
					let name = if super::is_identifier(name, &[]) {
						name.clone()
					} else {
						super::string(name)
					};
					let optional = if field.optional { "?" } else { "" };
					Ok(format!("{name}{optional}: {}", self::type_(&field.type_)?))
				})
				.collect::<tg::Result<Vec<_>>>()?;
			format!("{{ {} }}", fields.join("; "))
		},
		tg::interface::Type::Reference { import, name } => {
			if !super::is_identifier(name, RESERVED) {
				return Err(tg::error!(%name, "the name of the type is not an identifier"));
			}
			match import {
				Some(import) => format!("{import}.{name}"),
				None => name.clone(),
			}
		},
		tg::interface::Type::String => "string".to_owned(),
		tg::interface::Type::Symlink => "tg.Symlink".to_owned(),
		tg::interface::Type::Template => "tg.Template".to_owned(),
		tg::interface::Type::Tuple { elements, rest } => {
			let mut items = elements
				.iter()
				.map(|element| {
					let type_ = self::member(&element.type_)?;
					let optional = if element.optional { "?" } else { "" };
					Ok(format!("{type_}{optional}"))
				})
				.collect::<tg::Result<Vec<_>>>()?;
			if let Some(rest) = rest {
				items.push(format!("...Array<{}>", self::type_(rest)?));
			}
			format!("[{}]", items.join(", "))
		},
		tg::interface::Type::Union { types } => {
			if types.is_empty() {
				return Ok("never".to_owned());
			}
			types
				.iter()
				.map(self::member)
				.collect::<tg::Result<Vec<_>>>()?
				.join(" | ")
		},
		tg::interface::Type::Value => "tg.Value".to_owned(),
	};
	Ok(output)
}

/// Print a type as a member of a union or a tuple, with parentheses around a union with more than one member.
fn member(type_: &tg::interface::Type) -> tg::Result<String> {
	let output = self::type_(type_)?;
	let output = match type_ {
		tg::interface::Type::Union { types } if types.len() > 1 => format!("({output})"),
		_ => output,
	};
	Ok(output)
}
