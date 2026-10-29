use {crate::prelude::*, std::collections::BTreeMap};

#[cfg(test)]
mod tests;

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

impl Interface {
	pub fn serialize_json(&self) -> tg::Result<String> {
		serde_json::to_string_pretty(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the interface"))
	}

	pub fn deserialize_json(text: &str) -> tg::Result<Self> {
		serde_json::from_str(text)
			.map_err(|error| tg::error!(!error, "failed to deserialize the interface"))
	}
}
