use crate::{self as tg, error};
use derive_more::TryUnwrap;
use std::collections::BTreeMap;

/// An import in a module.
#[derive(Debug, Clone, PartialEq, Eq, Hash, TryUnwrap)]
#[try_unwrap(ref)]
pub enum Import {
	/// An import of a module, such as `import "./module.tg"`.
	Module(tg::Path),

	/// An import of a dependency, such as `import "tg:std"`.
	Dependency(tg::Dependency),
}

impl Import {
	pub fn with_specifier_and_attributes(
		specifier: &str,
		attributes: Option<&BTreeMap<String, String>>,
	) -> tg::Result<Self> {
		// Parse the specifier.
		let import = specifier.parse()?;

		// Merge with the attributes.
		let import = if let Some(attributes) = attributes {
			match import {
				Self::Module(module) => Self::Module(module),
				Self::Dependency(mut dependency) => {
					let attributes = attributes
						.iter()
						.map(|(key, value)| (key.clone(), value.clone()))
						.collect();
					let attributes = serde_json::from_value(attributes)
						.map_err(|source| error!(!source, "failed to parse the attributes"))?;
					dependency.merge(attributes);
					Self::Dependency(dependency)
				},
			}
		} else {
			import
		};

		Ok(import)
	}
}

impl std::fmt::Display for Import {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Import::Module(path) => {
				write!(f, "{path}")?;
			},

			Import::Dependency(dependency) => {
				write!(f, "tg:{dependency}")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Import {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		if value.starts_with('.') {
			let path = value.parse::<tg::Path>()?;
			if !matches!(path.extension(), Some("js" | "ts" | "tg.js" | "tg.ts")) {
				return Err(error!(
					%path,
					r#"the path does not have a valid extension"#
				));
			}
			Ok(Import::Module(path))
		} else if let Some(value) = value.strip_prefix("tg:") {
			let dependency = value.parse().map_err(
				|source| error!(!source, %value, "failed to parse value as a dependency"),
			)?;
			Ok(Import::Dependency(dependency))
		} else {
			return Err(error!(?value, r#"the import is not valid"#));
		}
	}
}

impl From<Import> for String {
	fn from(value: Import) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Import {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self, Self::Error> {
		value.parse()
	}
}
