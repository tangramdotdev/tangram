use {crate::prelude::*, std::collections::BTreeMap};

#[cfg(test)]
mod tests;

pub mod parse;
pub mod print;

/// The interface of a module, which describes its exports with types over values.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Interface {
	/// The named types that the exports refer to.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub definitions: BTreeMap<String, Type>,

	/// The exports, by name.
	pub exports: BTreeMap<String, Export>,

	/// The modules whose definitions the types refer to, by the names that references use.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub imports: BTreeMap<String, tg::module::Data>,
}

/// An export of a module.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Export {
	/// The export's documentation.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub documentation: Option<String>,

	/// The reason that the export is not a value, if it is not.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<String>,

	/// The range of the export's name in the module's source, with UTF-8 character offsets.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub range: Option<tg::Range>,

	/// The export's type, if it is a value.
	#[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
	pub type_: Option<Type>,
}

/// A type over values.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Type {
	/// An array whose items all have the same type.
	Array { item: Box<Type> },

	/// A directory, file, or symlink.
	Artifact,

	/// A blob.
	Blob,

	/// A boolean.
	Boolean,

	/// Bytes.
	Bytes,

	/// A command with the types of its args and output.
	Command { args: Box<Type>, output: Box<Type> },

	/// A directory.
	Directory,

	/// An error.
	Error,

	/// A file.
	File,

	/// A graph.
	Graph,

	/// A single boolean, number, or string.
	Literal { value: Literal },

	/// A map with string keys whose values all have the same type.
	Map { value: Box<Type> },

	/// A module.
	Module,

	/// A mutation.
	Mutation,

	/// Null.
	Null,

	/// A number.
	Number,

	/// Any object.
	Object,

	/// A placeholder.
	Placeholder,

	/// A map with a fixed set of fields.
	Record { fields: BTreeMap<String, Field> },

	/// A reference to a named type in the interface's definitions, or in the definitions of an imported module's interface.
	Reference {
		#[serde(default, skip_serializing_if = "Option::is_none")]
		import: Option<String>,

		name: String,
	},

	/// A string.
	String,

	/// A symlink.
	Symlink,

	/// A template.
	Template,

	/// An array with a fixed sequence of elements, optionally followed by any number of items with the same type.
	Tuple {
		elements: Vec<Element>,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		rest: Option<Box<Type>>,
	},

	/// A value that has one of the types.
	Union { types: Vec<Type> },

	/// Any value.
	Value,
}

/// A field of a record.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
	/// Whether the field may be absent.
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub optional: bool,

	/// The field's type.
	#[serde(rename = "type")]
	pub type_: Type,
}

/// An element of a tuple.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Element {
	/// Whether the element may be absent.
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub optional: bool,

	/// The element's type.
	#[serde(rename = "type")]
	pub type_: Type,
}

/// A literal value.
#[derive(Clone, Debug, PartialEq, derive_more::From, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Literal {
	Boolean(bool),
	Number(f64),
	String(String),
}

/// The language of an interface file, which its extension names: `.tgi.json`, `.tgi.py`, or `.tgi.ts`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, derive_more::Display)]
pub enum Language {
	#[display("json")]
	Json,

	#[display("python")]
	Python,

	#[default]
	#[display("typescript")]
	TypeScript,
}

impl Interface {
	pub fn serialize_json(&self) -> tg::Result<String> {
		serde_json::to_string_pretty(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the interface"))
	}

	pub fn deserialize_json(text: &str) -> tg::Result<Self> {
		serde_json::from_str(text)
			.map_err(|error| tg::error!(!error, "failed to deserialize the interface"))
	}

	/// Convert a value that a runtime returns to an interface.
	pub fn try_from_value(value: &tg::Value) -> tg::Result<Self> {
		let value = json(value)?;
		serde_json::from_value(value)
			.map_err(|error| tg::error!(argument, !error, "the value is not an interface"))
	}
}

impl Language {
	/// All of the languages.
	pub const ALL: [Self; 3] = [Self::Json, Self::Python, Self::TypeScript];

	/// Get the language of an interface module's kind.
	#[must_use]
	pub fn for_kind(kind: tg::module::Kind) -> Option<Self> {
		match kind {
			tg::module::Kind::InterfaceJson => Some(Self::Json),
			tg::module::Kind::InterfacePython => Some(Self::Python),
			tg::module::Kind::InterfaceTypeScript => Some(Self::TypeScript),
			_ => None,
		}
	}

	/// Get the language of an interface file from its path.
	#[must_use]
	pub fn for_path(path: impl AsRef<std::path::Path>) -> Option<Self> {
		let name = path.as_ref().file_name()?.to_str()?;
		Self::ALL
			.into_iter()
			.find(|language| name.ends_with(&format!(".tgi.{}", language.extension())))
	}

	/// Get the extension that follows `.tgi` in the file names of interfaces in the language.
	#[must_use]
	pub fn extension(self) -> &'static str {
		match self {
			Self::Json => "json",
			Self::Python => "py",
			Self::TypeScript => "ts",
		}
	}

	/// Get the kind of interface modules in the language.
	#[must_use]
	pub fn kind(self) -> tg::module::Kind {
		match self {
			Self::Json => tg::module::Kind::InterfaceJson,
			Self::Python => tg::module::Kind::InterfacePython,
			Self::TypeScript => tg::module::Kind::InterfaceTypeScript,
		}
	}
}

impl std::str::FromStr for Language {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self> {
		let language = match value {
			"json" => Self::Json,
			"python" => Self::Python,
			"typescript" => Self::TypeScript,
			_ => return Err(tg::error!(%value, "invalid interface language")),
		};
		Ok(language)
	}
}

/// Convert a value to JSON, rejecting values that JSON cannot represent.
fn json(value: &tg::Value) -> tg::Result<serde_json::Value> {
	let value = match value {
		tg::Value::Null => serde_json::Value::Null,
		tg::Value::Bool(value) => serde_json::Value::Bool(*value),
		tg::Value::Number(value) => serde_json::Number::from_f64(*value)
			.map(serde_json::Value::Number)
			.ok_or_else(|| tg::error!(argument, "the number is not finite"))?,
		tg::Value::String(value) => serde_json::Value::String(value.clone()),
		tg::Value::Array(values) => {
			serde_json::Value::Array(values.iter().map(json).collect::<tg::Result<_>>()?)
		},
		tg::Value::Map(map) => serde_json::Value::Object(
			map.iter()
				.map(|(key, value)| Ok((key.clone(), json(value)?)))
				.collect::<tg::Result<_>>()?,
		),
		_ => return Err(tg::error!(argument, "the value is not an interface")),
	};
	Ok(value)
}
