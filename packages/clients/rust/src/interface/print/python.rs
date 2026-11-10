use {crate::prelude::*, std::fmt::Write as _};

/// The words that cannot be the names of declarations in Python.
const RESERVED: &[&str] = &[
	"False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue",
	"def", "del", "elif", "else", "except", "finally", "for", "from", "global", "if", "import",
	"in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try", "type",
	"while", "with", "yield",
];

/// Print an interface in Python.
pub(super) fn print(interface: &tg::Interface) -> tg::Result<String> {
	let mut output = String::new();

	// Declare a dependency for each module that the exports re-export from, named after the module, unless an import or another dependency has the name.
	let mut dependencies = Vec::<(String, &tg::module::Import)>::new();
	for reexport in interface
		.exports
		.values()
		.filter_map(|export| export.reexport.as_ref())
	{
		if dependencies
			.iter()
			.any(|(_, import)| **import == reexport.import)
		{
			continue;
		}
		let mut dependency = self::dependency(&reexport.import);
		while !super::is_identifier(&dependency, RESERVED)
			|| interface.imports.contains_key(&dependency)
			|| dependencies.iter().any(|(other, _)| *other == dependency)
		{
			dependency.push('_');
		}
		dependencies.push((dependency, &reexport.import));
	}

	// Print the imports and the dependencies of the re-exports in a script metadata block.
	if !interface.imports.is_empty() || !dependencies.is_empty() {
		output.push_str("# /// script\n");
		for (alias, module) in &interface.imports {
			if !super::is_identifier(alias, RESERVED) {
				return Err(tg::error!(%alias, "the name of the import is not an identifier"));
			}
			let (specifier, kind) = super::import(module)?;
			writeln!(output, "# [tool.tangram.dependencies.{alias}]").unwrap();
			writeln!(output, "# specifier = {}", super::string(&specifier)).unwrap();
			if let Some(kind) = kind {
				writeln!(output, "# kind = {}", super::string(&kind)).unwrap();
			}
		}
		for (dependency, import) in &dependencies {
			let (specifier, kind) = super::reexport(import);
			writeln!(output, "# [tool.tangram.dependencies.{dependency}]").unwrap();
			writeln!(output, "# specifier = {}", super::string(&specifier)).unwrap();
			if let Some(kind) = kind {
				writeln!(output, "# kind = {}", super::string(&kind)).unwrap();
			}
		}
		output.push_str("# ///\n");
	}

	// Print the definitions, as classes for records whose fields are named with identifiers, and with the functional syntax for other records.
	for (name, type_) in &interface.definitions {
		if !super::is_identifier(name, RESERVED) {
			return Err(tg::error!(%name, "the name of the type is not an identifier"));
		}
		output.push('\n');
		match type_ {
			tg::interface::Type::Record { fields }
				if fields
					.keys()
					.all(|name| super::is_identifier(name, RESERVED)) =>
			{
				writeln!(output, "class {name}(TypedDict, closed=True):").unwrap();
				if fields.is_empty() {
					output.push_str("    pass\n");
				}
				for (name, field) in fields {
					writeln!(output, "    {name}: {}", self::field(field)?).unwrap();
				}
			},
			tg::interface::Type::Record { fields } => {
				let quoted = super::string(name);
				let fields = fields
					.iter()
					.map(|(name, field)| {
						Ok(format!("{}: {}", super::string(name), self::field(field)?))
					})
					.collect::<tg::Result<Vec<_>>>()?
					.join(", ");
				writeln!(
					output,
					"{name} = TypedDict({quoted}, {{{fields}}}, closed=True)"
				)
				.unwrap();
			},
			_ => writeln!(output, "type {name} = {}", self::type_(type_)?).unwrap(),
		}
	}

	// Print the exports, importing re-exported namespaces and exports from their dependencies, with their documentation as docstrings on the lines after them.
	for (name, export) in &interface.exports {
		if !super::is_identifier(name, RESERVED) {
			return Err(tg::error!(%name, "the name of the export is not an identifier"));
		}
		output.push('\n');
		if let Some(reexport) = &export.reexport {
			let (dependency, _) = dependencies
				.iter()
				.find(|(_, import)| **import == reexport.import)
				.unwrap();
			match &reexport.export {
				None if dependency == name => writeln!(output, "import {name}"),
				None => writeln!(output, "import {dependency} as {name}"),
				Some(member) if !super::is_identifier(member, RESERVED) => {
					return Err(tg::error!(
						%member,
						"the name of the re-exported export is not an identifier"
					));
				},
				Some(member) if member == name => {
					writeln!(output, "from {dependency} import {name}")
				},
				Some(member) => writeln!(output, "from {dependency} import {member} as {name}"),
			}
			.unwrap();
		} else {
			let type_ = match (&export.error, &export.type_) {
				(Some(reason), _) => format!("tg.NonValue[{}]", super::string(reason)),
				(None, Some(type_)) => self::type_(type_)?,
				(None, None) => "tg.Value".to_owned(),
			};
			writeln!(output, "{name}: {type_}").unwrap();
		}
		if let Some(documentation) = &export.documentation {
			writeln!(output, "{}", super::string(documentation)).unwrap();
		}
	}

	let output = output.trim_start_matches('\n').to_owned();

	Ok(output)
}

/// Name the dependency of a re-exported module after the module's file or package.
fn dependency(import: &tg::module::Import) -> String {
	let name = match import.reference.node() {
		tg::reference::Node::Path(path) => path
			.file_name()
			.and_then(|name| name.to_str())
			.and_then(|name| name.split('.').next())
			.unwrap_or_default()
			.to_owned(),
		tg::reference::Node::Specifier(specifier) => specifier
			.to_string()
			.split('/')
			.next()
			.unwrap_or_default()
			.to_owned(),
		tg::reference::Node::Id(_) | tg::reference::Node::Pointer(_) => String::new(),
	};
	let name = name
		.chars()
		.map(|character| {
			if character.is_ascii_alphanumeric() || character == '_' {
				character
			} else {
				'_'
			}
		})
		.collect::<String>();
	if name.is_empty() || name.starts_with(|character: char| character.is_ascii_digit()) {
		format!("module{name}")
	} else {
		name
	}
}

fn field(field: &tg::interface::Field) -> tg::Result<String> {
	let type_ = self::type_(&field.type_)?;
	let output = if field.optional {
		format!("NotRequired[{type_}]")
	} else {
		type_
	};
	Ok(output)
}

fn type_(type_: &tg::interface::Type) -> tg::Result<String> {
	let output = match type_ {
		tg::interface::Type::Array { item } => format!("list[{}]", self::type_(item)?),
		tg::interface::Type::Artifact => "tg.Artifact".to_owned(),
		tg::interface::Type::Blob => "tg.Blob".to_owned(),
		tg::interface::Type::Boolean => "bool".to_owned(),
		tg::interface::Type::Bytes => "bytes".to_owned(),
		tg::interface::Type::Command { args, output } => {
			format!(
				"tg.Command[{}, {}]",
				self::type_(args)?,
				self::type_(output)?
			)
		},
		tg::interface::Type::Directory => "tg.Directory".to_owned(),
		tg::interface::Type::Error => "tg.Error".to_owned(),
		tg::interface::Type::File => "tg.File".to_owned(),
		tg::interface::Type::Graph => "tg.Graph".to_owned(),
		tg::interface::Type::Literal { value } => {
			let value = match value {
				tg::interface::Literal::Boolean(true) => "True".to_owned(),
				tg::interface::Literal::Boolean(false) => "False".to_owned(),
				tg::interface::Literal::Number(value) => super::number(*value)?,
				tg::interface::Literal::String(value) => super::string(value),
			};
			format!("Literal[{value}]")
		},
		tg::interface::Type::Map { value } => format!("dict[str, {}]", self::type_(value)?),
		tg::interface::Type::Module => "tg.Module".to_owned(),
		tg::interface::Type::Mutation => "tg.Mutation".to_owned(),
		tg::interface::Type::Null => "None".to_owned(),
		tg::interface::Type::Number => "float".to_owned(),
		tg::interface::Type::Object => "tg.Object".to_owned(),
		tg::interface::Type::Placeholder => "tg.Placeholder".to_owned(),
		// Widen a record without a name to a value, because Python has no inline record type.
		tg::interface::Type::Record { .. } | tg::interface::Type::Value => "tg.Value".to_owned(),
		tg::interface::Type::Reference { import, name } => {
			if !super::is_identifier(name, RESERVED) {
				return Err(tg::error!(%name, "the name of the type is not an identifier"));
			}
			match import {
				Some(import) => format!("{import}.{name}"),
				None => name.clone(),
			}
		},
		tg::interface::Type::String => "str".to_owned(),
		tg::interface::Type::Symlink => "tg.Symlink".to_owned(),
		tg::interface::Type::Template => "tg.Template".to_owned(),
		tg::interface::Type::Tuple { elements, rest } => {
			if elements.is_empty() {
				return Ok(match rest {
					Some(rest) => format!("tuple[{}, ...]", self::type_(rest)?),
					None => "tuple[()]".to_owned(),
				});
			}
			let mut items = elements
				.iter()
				.map(|element| {
					let field = tg::interface::Field {
						optional: element.optional,
						type_: element.type_.clone(),
					};
					self::field(&field)
				})
				.collect::<tg::Result<Vec<_>>>()?;
			if let Some(rest) = rest {
				items.push(format!("*tuple[{}, ...]", self::type_(rest)?));
			}
			format!("tuple[{}]", items.join(", "))
		},
		tg::interface::Type::Union { types } => {
			if types.is_empty() {
				return Ok("Never".to_owned());
			}
			types
				.iter()
				.map(|member| {
					let output = self::type_(member)?;
					let output = match member {
						tg::interface::Type::Union { types } if types.len() > 1 => {
							format!("({output})")
						},
						_ => output,
					};
					Ok(output)
				})
				.collect::<tg::Result<Vec<_>>>()?
				.join(" | ")
		},
	};
	Ok(output)
}
