use {
	ruff_python_ast::{Expr, Stmt},
	std::{
		cell::RefCell,
		collections::{BTreeMap, BTreeSet},
	},
	tangram_client::prelude::*,
};

/// The names that printed Python types use.
pub(super) struct Names {
	/// The name of the Tangram client module.
	pub(super) client: String,

	/// The names of the interface's definitions.
	pub(super) definitions: BTreeMap<String, String>,

	/// The prefix of the names of generated helpers.
	pub(super) prefix: String,

	/// The statements that define the typed dicts of records, which are printed before the types that use them.
	pub(super) records: RefCell<Vec<String>>,

	/// The name of the typing module.
	pub(super) typing: String,
}

/// Escape keywords without displacing an export that already has a valid Python name.
pub(super) fn binding(name: &str, exports: &BTreeSet<String>) -> tg::Result<String> {
	if name == "__all__" {
		return Err(tg::error!(
			"the export name __all__ is reserved by the python loader"
		));
	}
	if is_identifier(name) {
		return Ok(name.to_owned());
	}
	let mut binding = format!("{name}_");
	if !is_identifier(&binding) {
		return Err(tg::error!(export = %name, "the export name is not a python identifier"));
	}
	while exports.contains(&binding) {
		binding.push('_');
	}
	Ok(binding)
}

fn is_identifier(name: &str) -> bool {
	let assignment = format!("{name} = None");
	let Ok(parsed) = ruff_python_parser::parse_module(&assignment) else {
		return false;
	};
	matches!(parsed.syntax().body.as_slice(), [Stmt::Assign(assign)] if matches!(assign.targets.as_slice(), [Expr::Name(target)] if target.id.as_str() == name))
}

/// Print the type of an export as a Python type expression, giving an export that is not a value a type that carries the reason.
pub(super) fn export(export: &tg::interface::Export, names: &Names) -> String {
	let Some(type_) = &export.type_ else {
		let client = &names.client;
		let typing = &names.typing;
		// Replace the characters that a string annotation cannot contain without an escape sequence.
		let error = export
			.error
			.as_deref()
			.unwrap_or("the export is not a value")
			.chars()
			.map(|character| match character {
				'"' | '\'' => '`',
				'\\' => '/',
				character if character.is_control() => ' ',
				character => character,
			})
			.collect::<String>();
		return format!("{client}.NonValue[{typing}.Literal['{error}']]");
	};
	self::type_(type_, names)
}

/// Print a type as a Python type expression.
pub(super) fn type_(type_: &tg::interface::Type, names: &Names) -> String {
	let client = &names.client;
	let typing = &names.typing;
	match type_ {
		tg::interface::Type::Array { item } => format!("list[{}]", self::type_(item, names)),
		tg::interface::Type::Artifact => format!("{client}.Artifact"),
		tg::interface::Type::Blob => format!("{client}.Blob"),
		tg::interface::Type::Boolean => "bool".to_owned(),
		tg::interface::Type::Bytes => "bytes".to_owned(),
		tg::interface::Type::Command { args, output } => {
			let args = match args.as_ref() {
				tg::interface::Type::Array { item } => {
					format!("tuple[{}, ...]", self::type_(item, names))
				},
				tg::interface::Type::Tuple { elements, rest } => {
					args_tuple(elements, rest.as_deref(), names)
				},
				_ => format!("tuple[{client}.Value.Type, ...]"),
			};
			let output = self::type_(output, names);
			format!("{client}.Command[{args}, {output}]")
		},
		tg::interface::Type::Directory => format!("{client}.Directory"),
		tg::interface::Type::Error => format!("{client}.Error"),
		tg::interface::Type::File => format!("{client}.File"),
		tg::interface::Type::Graph => format!("{client}.Graph"),
		tg::interface::Type::Literal { value } => match value {
			tg::interface::Literal::Boolean(true) => format!("{typing}.Literal[True]"),
			tg::interface::Literal::Boolean(false) => format!("{typing}.Literal[False]"),
			tg::interface::Literal::Number(_) => "float".to_owned(),
			tg::interface::Literal::String(value) => {
				let value = serde_json::to_string(value).unwrap();
				format!("{typing}.Literal[{value}]")
			},
		},
		tg::interface::Type::Map { value } => format!("dict[str, {}]", self::type_(value, names)),
		tg::interface::Type::Module => format!("{client}.Module"),
		tg::interface::Type::Mutation => format!("{client}.Mutation"),
		tg::interface::Type::Null => "None".to_owned(),
		tg::interface::Type::Number => "float".to_owned(),
		tg::interface::Type::Object => format!("{client}.Object"),
		tg::interface::Type::Placeholder => format!("{client}.Placeholder"),
		tg::interface::Type::Record { fields } => {
			// Define a typed dict whose field types are forward references, because the functional syntax evaluates its fields.
			let fields = fields
				.iter()
				.map(|(name, field)| {
					let name = serde_json::to_string(name).unwrap();
					let type_ = self::type_(&field.type_, names);
					let type_ = if field.optional {
						format!("{typing}.NotRequired[{type_}]")
					} else {
						type_
					};
					let type_ = serde_json::to_string(&type_).unwrap();
					format!("{name}: {type_}")
				})
				.collect::<Vec<_>>();
			let mut records = names.records.borrow_mut();
			let record = format!("{}_record_{}", names.prefix, records.len());
			let quoted = serde_json::to_string(&record).unwrap();
			let fields = fields.join(", ");
			records.push(format!(
				"{record} = {typing}.TypedDict({quoted}, {{{fields}}})"
			));
			record
		},
		tg::interface::Type::Reference { name } => names
			.definitions
			.get(name)
			.cloned()
			.unwrap_or_else(|| format!("{client}.Value.Type")),
		tg::interface::Type::String => "str".to_owned(),
		tg::interface::Type::Symlink => format!("{client}.Symlink"),
		tg::interface::Type::Template => format!("{client}.Template"),
		tg::interface::Type::Tuple { elements, rest } => {
			// Express optional elements as a union of the tuples that end at each optional element.
			let required = elements
				.iter()
				.take_while(|element| !element.optional)
				.count();
			let rest = rest
				.as_ref()
				.map(|rest| format!("*tuple[{}, ...]", self::type_(rest, names)));
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
				let tuple = if items.is_empty() {
					"tuple[()]".to_owned()
				} else {
					format!("tuple[{}]", items.join(", "))
				};
				tuples.push(tuple);
			}
			tuples.join(" | ")
		},
		tg::interface::Type::Union { types } => {
			if types.is_empty() {
				return format!("{typing}.Never");
			}
			let types = types
				.iter()
				.map(|member| self::type_(member, names))
				.collect::<Vec<_>>();
			format!("({})", types.join(" | "))
		},
		tg::interface::Type::Value => format!("{client}.Value.Type"),
	}
}

/// Print the args of a command as a single tuple type, which a command's typed build method can bind to its parameters, by widening optional elements to a variadic tail.
fn args_tuple(
	elements: &[tg::interface::Element],
	rest: Option<&tg::interface::Type>,
	names: &Names,
) -> String {
	let required = elements
		.iter()
		.take_while(|element| !element.optional)
		.count();
	let mut items = elements[..required]
		.iter()
		.map(|element| self::type_(&element.type_, names))
		.collect::<Vec<_>>();
	let mut tail = elements[required..]
		.iter()
		.map(|element| self::type_(&element.type_, names))
		.collect::<Vec<_>>();
	if let Some(rest) = rest {
		tail.push(self::type_(rest, names));
	}
	tail.dedup();
	match tail.as_slice() {
		[] => (),
		[item] => items.push(format!("*tuple[{item}, ...]")),
		_ => items.push(format!("*tuple[{}, ...]", tail.join(" | "))),
	}
	if items.is_empty() {
		"tuple[()]".to_owned()
	} else {
		format!("tuple[{}]", items.join(", "))
	}
}
