use {std::collections::BTreeMap, tangram_client::prelude::*};

/// The names that printed TypeScript types use.
pub(super) struct Names {
	/// The names of the interface's definitions.
	pub(super) definitions: BTreeMap<String, String>,

	/// The names of the namespaces that import the modules that the interface imports.
	pub(super) imports: BTreeMap<String, String>,
}

/// Print the type of an export as TypeScript, giving an export that is not a value a type that carries the reason.
pub(super) fn export(export: &tg::interface::Export, names: &Names) -> String {
	let Some(type_) = &export.type_ else {
		let error = export
			.error
			.as_deref()
			.unwrap_or("the export is not a value");
		let error = serde_json::to_string(error).unwrap();
		return format!("tg.NonValue<{error}>");
	};
	self::type_(type_, names)
}

/// Determine whether a name is a TypeScript identifier that can name a type.
pub(super) fn is_identifier(name: &str) -> bool {
	let mut characters = name.chars();
	characters
		.next()
		.is_some_and(|character| character.is_ascii_alphabetic() || matches!(character, '_' | '$'))
		&& characters
			.all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '$'))
		&& !matches!(
			name,
			"any"
				| "bigint" | "boolean"
				| "break" | "case"
				| "catch" | "class"
				| "const" | "continue"
				| "debugger" | "default"
				| "delete" | "do"
				| "else" | "enum"
				| "export" | "extends"
				| "false" | "finally"
				| "for" | "function"
				| "if" | "implements"
				| "import" | "in"
				| "instanceof"
				| "interface"
				| "let" | "never"
				| "new" | "null"
				| "number" | "object"
				| "package" | "private"
				| "protected"
				| "public" | "return"
				| "static" | "string"
				| "super" | "switch"
				| "symbol" | "this"
				| "throw" | "true"
				| "try" | "typeof"
				| "undefined"
				| "unknown" | "var"
				| "void" | "while"
				| "with" | "yield"
		)
}

/// Print a type as TypeScript, naming the interface's definitions with the given names.
pub(super) fn type_(type_: &tg::interface::Type, names: &Names) -> String {
	match type_ {
		tg::interface::Type::Array { item } => format!("Array<{}>", self::type_(item, names)),
		tg::interface::Type::Artifact => "tg.Artifact".to_owned(),
		tg::interface::Type::Blob => "tg.Blob".to_owned(),
		tg::interface::Type::Boolean => "boolean".to_owned(),
		tg::interface::Type::Bytes => "Uint8Array".to_owned(),
		tg::interface::Type::Command { args, output } => {
			let args = match args.as_ref() {
				tg::interface::Type::Array { .. } | tg::interface::Type::Tuple { .. } => {
					self::type_(args, names)
				},
				_ => "Array<tg.Value>".to_owned(),
			};
			let output = self::type_(output, names);
			format!("tg.Command<{args}, {output}>")
		},
		tg::interface::Type::Directory => "tg.Directory".to_owned(),
		tg::interface::Type::Error => "tg.Error".to_owned(),
		tg::interface::Type::File => "tg.File".to_owned(),
		tg::interface::Type::Graph => "tg.Graph".to_owned(),
		tg::interface::Type::Literal { value } => literal(value),
		tg::interface::Type::Map { value } => {
			format!("{{ [key: string]: {} }}", self::type_(value, names))
		},
		tg::interface::Type::Module => "tg.Module".to_owned(),
		tg::interface::Type::Mutation => "tg.Mutation".to_owned(),
		tg::interface::Type::Null => "null".to_owned(),
		tg::interface::Type::Number => "number".to_owned(),
		tg::interface::Type::Object => "tg.Object".to_owned(),
		tg::interface::Type::Placeholder => "tg.Placeholder".to_owned(),
		tg::interface::Type::Record { fields } => {
			let fields = fields
				.iter()
				.map(|(name, field)| {
					let name = serde_json::to_string(name).unwrap();
					let optional = if field.optional { "?" } else { "" };
					let type_ = self::type_(&field.type_, names);
					format!("{name}{optional}: {type_}")
				})
				.collect::<Vec<_>>();
			if fields.is_empty() {
				"{}".to_owned()
			} else {
				format!("{{ {} }}", fields.join("; "))
			}
		},
		tg::interface::Type::Reference { import: None, name } => names
			.definitions
			.get(name)
			.cloned()
			.unwrap_or_else(|| "tg.Value".to_owned()),
		tg::interface::Type::Reference {
			import: Some(import),
			name,
		} => names
			.imports
			.get(import)
			.map_or_else(|| "tg.Value".to_owned(), |alias| format!("{alias}.{name}")),
		tg::interface::Type::String => "string".to_owned(),
		tg::interface::Type::Symlink => "tg.Symlink".to_owned(),
		tg::interface::Type::Template => "tg.Template".to_owned(),
		tg::interface::Type::Tuple { elements, rest } => {
			// Express optional elements as a union of the tuples that end at each optional element, because an optional element's type includes undefined, which is not a value.
			let required = elements
				.iter()
				.take_while(|element| !element.optional)
				.count();
			let rest = rest
				.as_ref()
				.map(|rest| format!("...Array<{}>", self::type_(rest, names)));
			let mut tuples = Vec::new();
			for length in required..=elements.len() {
				let mut items = elements[..length]
					.iter()
					.map(|element| self::type_(&element.type_, names))
					.collect::<Vec<_>>();
				if length == elements.len()
					&& let Some(rest) = &rest
				{
					items.push(rest.clone());
				}
				tuples.push(format!("[{}]", items.join(", ")));
			}
			if let [tuple] = tuples.as_slice() {
				tuple.clone()
			} else {
				format!("({})", tuples.join(" | "))
			}
		},
		tg::interface::Type::Union { types } => {
			if types.is_empty() {
				return "never".to_owned();
			}
			let types = types
				.iter()
				.map(|member| self::type_(member, names))
				.collect::<Vec<_>>();
			format!("({})", types.join(" | "))
		},
		tg::interface::Type::Value => "tg.Value".to_owned(),
	}
}

fn literal(value: &tg::interface::Literal) -> String {
	match value {
		tg::interface::Literal::Boolean(value) => value.to_string(),
		tg::interface::Literal::Number(value) if value.is_finite() => value.to_string(),
		tg::interface::Literal::Number(_) => "number".to_owned(),
		tg::interface::Literal::String(value) => serde_json::to_string(value).unwrap(),
	}
}
