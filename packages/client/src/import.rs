use crate as tg;
use std::{collections::BTreeMap, fmt};

/// An import in a module.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Import {
	/// The import specifier.
	pub specifier: Specifier,

	/// The type of the import.
	pub r#type: Option<Type>,
}

/// An import specifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Specifier {
	/// A path to a module, like "./module.ts"
	Path(tg::Path),

	/// A dependency, like "tg:package@version"
	Dependency(tg::Dependency),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Type {
	Js,
	Ts,
	Dts,
	Artifact,
	Directory,
	File,
	Symlink,
}

impl Import {
	pub fn with_specifier_and_attributes(
		specifier: &str,
		attributes: Option<&BTreeMap<String, String>>,
	) -> tg::Result<Self> {
		// Parse the specifier.
		let mut specifier = specifier.parse()?;

		// Merge attributes.
		if let (Some(attributes), Specifier::Dependency(dependency)) =
			(attributes.cloned(), &mut specifier)
		{
			let attributes = attributes.into_iter().collect();
			let attributes = serde_json::from_value(attributes)
				.map_err(|source| tg::error!(!source, "failed to parse the attributes"))?;
			dependency.merge(attributes);
		}

		// Parse the type.
		let attributes = attributes.cloned().unwrap_or_default();
		let r#type = attributes.get("type").map(String::as_str);
		let r#type = match r#type {
			Some("js") => Some(Type::Js),
			Some("ts") => Some(Type::Ts),
			Some("dts") => Some(Type::Dts),
			Some("artifact") => Some(Type::Artifact),
			Some("directory") => Some(Type::Directory),
			Some("file") => Some(Type::File),
			Some("symlink") => Some(Type::Symlink),
			Some(r#type) => return Err(tg::error!("unknown import type: {type}")),
			None => match &specifier {
				Specifier::Path(path) => Type::try_from_path(path),
				Specifier::Dependency(_) => None,
			},
		};

		Ok(Import { specifier, r#type })
	}
}

impl Type {
	#[must_use]
	pub fn try_from_path(path: &tg::Path) -> Option<Self> {
		if path.as_str().to_lowercase().ends_with(".d.ts") {
			Some(Self::Dts)
		} else if path.as_str().to_lowercase().ends_with(".js") {
			Some(Self::Js)
		} else if path.as_str().to_lowercase().ends_with(".ts") {
			Some(Self::Ts)
		} else {
			None
		}
	}
}

impl std::str::FromStr for Specifier {
	type Err = tg::Error;
	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		if value.starts_with('.') {
			let path = value.parse::<tg::Path>()?;
			Ok(Specifier::Path(path))
		} else if let Some(value) = value.strip_prefix("tg:") {
			let dependency = value
				.parse()
				.map_err(|source| tg::error!(!source, %value, "invalid dependency"))?;
			Ok(Specifier::Dependency(dependency))
		} else {
			Err(tg::error!(?value, "invalid import"))
		}
	}
}

impl fmt::Display for Specifier {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Path(path) => write!(f, "{path}"),
			Self::Dependency(dependency) => write!(f, "tg:{dependency}"),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{Specifier, Type};
	use crate as tg;

	#[test]
	fn specifier() {
		let path_specifier = "./path/to/module.txt";
		let specifier = path_specifier.parse::<Specifier>().unwrap();
		assert_eq!(
			specifier,
			Specifier::Path("./path/to/module.txt".parse().unwrap())
		);
		assert_eq!(specifier.to_string().as_str(), path_specifier);

		let dependency_specifier = "tg:foo@1.2.3";
		let specifier = dependency_specifier.parse::<Specifier>().unwrap();
		assert_eq!(
			specifier,
			Specifier::Dependency(tg::Dependency::with_name_and_version(
				"foo".to_owned(),
				"1.2.3".to_owned()
			))
		);
		assert_eq!(specifier.to_string().as_str(), dependency_specifier);

		let invalid_specifier = "path/to/thing";
		invalid_specifier
			.parse::<Specifier>()
			.expect_err("Expected an invalid specifier");
	}

	#[test]
	fn import_with_attributes() {
		let path_specifier = "./module.ts";
		let attributes = [("type".to_owned(), "ts".to_owned())].into_iter().collect();
		let import =
			tg::Import::with_specifier_and_attributes(path_specifier, Some(&attributes)).unwrap();
		assert_eq!(
			import.specifier,
			Specifier::Path("./module.ts".parse().unwrap())
		);
		assert_eq!(import.r#type, Some(Type::Ts));

		let dependency_specifier = "tg:foo";
		let attributes = [
			("type".to_owned(), "directory".to_owned()),
			("version".to_owned(), "1.2.3".to_owned()),
		]
		.into_iter()
		.collect();
		let import =
			tg::Import::with_specifier_and_attributes(dependency_specifier, Some(&attributes))
				.unwrap();

		assert_eq!(
			import.specifier,
			Specifier::Dependency(tg::Dependency::with_name_and_version(
				"foo".to_owned(),
				"1.2.3".to_owned()
			))
		);
		assert_eq!(import.r#type, Some(Type::Directory));
	}
}
