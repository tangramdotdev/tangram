use {std::collections::BTreeMap, tangram_client::prelude::*};

/// Print the type of an export as TypeScript, giving an export that is not a value a type that carries the reason.
pub(super) fn export(
	export: &tg::interface::Export,
	definitions: &BTreeMap<String, String>,
) -> String {
	let Some(type_) = &export.type_ else {
		let error = export
			.error
			.as_deref()
			.unwrap_or("the export is not a value");
		let error = serde_json::to_string(error).unwrap();
		return format!("tg.NonValue<{error}>");
	};
	self::type_(type_, definitions)
}

/// Print a type as TypeScript, naming the interface's definitions with the given names.
pub(super) fn type_(type_: &tg::interface::Type, definitions: &BTreeMap<String, String>) -> String {
	match type_ {
		tg::interface::Type::Array { item } => format!("Array<{}>", self::type_(item, definitions)),
		tg::interface::Type::Artifact => "tg.Artifact".to_owned(),
		tg::interface::Type::Blob => "tg.Blob".to_owned(),
		tg::interface::Type::Boolean => "boolean".to_owned(),
		tg::interface::Type::Bytes => "Uint8Array".to_owned(),
		tg::interface::Type::Command { args, output } => {
			let args = match args.as_ref() {
				tg::interface::Type::Array { .. } | tg::interface::Type::Tuple { .. } => {
					self::type_(args, definitions)
				},
				_ => "Array<tg.Value>".to_owned(),
			};
			let output = self::type_(output, definitions);
			format!("tg.Command<{args}, {output}>")
		},
		tg::interface::Type::Directory => "tg.Directory".to_owned(),
		tg::interface::Type::Error => "tg.Error".to_owned(),
		tg::interface::Type::File => "tg.File".to_owned(),
		tg::interface::Type::Graph => "tg.Graph".to_owned(),
		tg::interface::Type::Literal { value } => literal(value),
		tg::interface::Type::Map { value } => {
			format!("{{ [key: string]: {} }}", self::type_(value, definitions))
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
					let type_ = self::type_(&field.type_, definitions);
					format!("{name}{optional}: {type_}")
				})
				.collect::<Vec<_>>();
			if fields.is_empty() {
				"{}".to_owned()
			} else {
				format!("{{ {} }}", fields.join("; "))
			}
		},
		tg::interface::Type::Reference { name } => definitions
			.get(name)
			.cloned()
			.unwrap_or_else(|| "tg.Value".to_owned()),
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
				.map(|rest| format!("...Array<{}>", self::type_(rest, definitions)));
			let mut tuples = Vec::new();
			for length in required..=elements.len() {
				let mut items = elements[..length]
					.iter()
					.map(|element| self::type_(&element.type_, definitions))
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
				.map(|member| self::type_(member, definitions))
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
