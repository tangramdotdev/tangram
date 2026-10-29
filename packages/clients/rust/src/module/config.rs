use {
	crate::prelude::*,
	std::{collections::BTreeMap, path::Path},
};

#[cfg(test)]
mod tests;

/// The config of a config module, which names the runtime that runs the module and the module's dependencies.
#[derive(Clone, Debug)]
pub struct Config {
	/// The args that precede the caller's args to the runtime.
	pub args: Vec<tg::Value>,

	/// The dependencies, by name.
	pub dependencies: BTreeMap<String, Dependency>,

	/// The runtime.
	pub runtime: Dependency,
}

/// A dependency of a config module, as a specifier or as a map with a specifier and attributes.
#[derive(Clone, Debug, PartialEq)]
pub enum Dependency {
	Map {
		attributes: BTreeMap<String, String>,
		specifier: String,
	},
	String(String),
}

/// The format of a config module.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Format {
	Json,
	Toml,
	Yaml,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Raw {
	#[serde(default)]
	args: Vec<serde_json::Value>,

	#[serde(default)]
	dependencies: BTreeMap<String, RawDependency>,

	runtime: RawDependency,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum RawDependency {
	Map(RawDependencyMap),
	String(String),
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDependencyMap {
	#[serde(default)]
	attributes: BTreeMap<String, String>,

	specifier: String,
}

impl Config {
	/// Parse a config in a format, rejecting unknown keys and values that are not Tangram values.
	pub fn parse(text: &str, format: Format) -> tg::Result<Self> {
		// Parse the text as a generic value.
		let value = match format {
			Format::Json => serde_json::from_str::<serde_json::Value>(text).map_err(|error| {
				tg::error!(argument, !error, "failed to parse the config as json")
			})?,
			Format::Toml => toml::from_str::<serde_json::Value>(text).map_err(|error| {
				tg::error!(argument, !error, "failed to parse the config as toml")
			})?,
			Format::Yaml => serde_yaml::from_str::<serde_json::Value>(text).map_err(|error| {
				tg::error!(argument, !error, "failed to parse the config as yaml")
			})?,
		};

		// Validate the schema.
		let raw = serde_json::from_value::<Raw>(value)
			.map_err(|error| tg::error!(argument, !error, "invalid config"))?;
		let args = raw
			.args
			.into_iter()
			.map(value_from_json)
			.collect::<tg::Result<_>>()?;
		let dependencies = raw
			.dependencies
			.into_iter()
			.map(|(name, dependency)| (name, dependency.into()))
			.collect();
		let runtime = raw.runtime.into();
		let config = Self {
			args,
			dependencies,
			runtime,
		};

		Ok(config)
	}
}

impl Dependency {
	/// Get the import of the dependency.
	pub fn import(&self) -> tg::Result<tg::module::Import> {
		let (specifier, attributes) = match self {
			Self::Map {
				attributes,
				specifier,
			} => (specifier.as_str(), Some(attributes.clone())),
			Self::String(specifier) => (specifier.as_str(), None),
		};
		tg::module::Import::with_specifier_and_attributes(specifier, attributes)
			.map_err(|error| tg::error!(argument, !error, %specifier, "invalid dependency"))
	}
}

impl Format {
	/// Get the format of a config module's kind.
	#[must_use]
	pub fn for_kind(kind: tg::module::Kind) -> Option<Self> {
		match kind {
			tg::module::Kind::Json => Some(Self::Json),
			tg::module::Kind::Toml => Some(Self::Toml),
			tg::module::Kind::Yaml => Some(Self::Yaml),
			_ => None,
		}
	}

	/// Get the kind of a config module in the format.
	#[must_use]
	pub fn kind(self) -> tg::module::Kind {
		match self {
			Self::Json => tg::module::Kind::Json,
			Self::Toml => tg::module::Kind::Toml,
			Self::Yaml => tg::module::Kind::Yaml,
		}
	}

	/// Get the format of a config module from its path.
	#[must_use]
	pub fn for_path(path: impl AsRef<Path>) -> Option<Self> {
		let name = path.as_ref().file_name()?.to_str()?;
		let format = if name == "tangram.json" || name.ends_with(".tg.json") {
			Self::Json
		} else if name == "tangram.toml" || name.ends_with(".tg.toml") {
			Self::Toml
		} else if name == "tangram.yaml" || name.ends_with(".tg.yaml") {
			Self::Yaml
		} else {
			return None;
		};
		Some(format)
	}
}

impl From<RawDependency> for Dependency {
	fn from(value: RawDependency) -> Self {
		match value {
			RawDependency::Map(map) => Self::Map {
				attributes: map.attributes,
				specifier: map.specifier,
			},
			RawDependency::String(specifier) => Self::String(specifier),
		}
	}
}

/// Convert a generic value to a Tangram value, rejecting a TOML datetime, which is not a Tangram value.
fn value_from_json(value: serde_json::Value) -> tg::Result<tg::Value> {
	let value = match value {
		serde_json::Value::Null => tg::Value::Null,
		serde_json::Value::Bool(value) => tg::Value::Bool(value),
		serde_json::Value::Number(value) => {
			let value = value
				.as_f64()
				.ok_or_else(|| tg::error!(argument, %value, "the number is not a value"))?;
			tg::Value::Number(value)
		},
		serde_json::Value::String(value) => tg::Value::String(value),
		serde_json::Value::Array(values) => {
			let values = values
				.into_iter()
				.map(value_from_json)
				.collect::<tg::Result<_>>()?;
			tg::Value::Array(values)
		},
		serde_json::Value::Object(map) => {
			if map.contains_key("$__toml_private_datetime") {
				return Err(tg::error!(argument, "a datetime is not a value"));
			}
			let map = map
				.into_iter()
				.map(|(key, value)| Ok((key, value_from_json(value)?)))
				.collect::<tg::Result<_>>()?;
			tg::Value::Map(map)
		},
	};
	Ok(value)
}
