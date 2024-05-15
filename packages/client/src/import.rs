use crate as tg;
use std::{collections::BTreeMap, fmt};

/// An import in a module.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Import {
	/// The import specifier.
	pub specifier: Specifier,

	/// The kind of the import.
	pub kind: Option<Kind>,
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
pub enum Kind {
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
		let kind = attributes.get("type").map(String::as_str);
		let kind = match kind {
			Some("js") => Some(Kind::Js),
			Some("ts") => Some(Kind::Ts),
			Some("dts") => Some(Kind::Dts),
			Some("artifact") => Some(Kind::Artifact),
			Some("directory") => Some(Kind::Directory),
			Some("file") => Some(Kind::File),
			Some("symlink") => Some(Kind::Symlink),
			Some(kind) => return Err(tg::error!(%kind, "unknown import type")),
			None => match &specifier {
				Specifier::Path(path) => Kind::try_from_path(path),
				Specifier::Dependency(_) => None,
			},
		};

		Ok(Import { specifier, kind })
	}
}

impl Kind {
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
	use super::{Kind, Specifier};
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
		assert_eq!(import.kind, Some(Kind::Ts));

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
		assert_eq!(import.kind, Some(Kind::Directory));
	}
}
