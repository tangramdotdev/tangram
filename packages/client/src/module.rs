use super::document::Document;
use crate::{self as tg, error};
use derive_more::{TryUnwrap, Unwrap};
use url::Url;

/// A module.
#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	Unwrap,
	TryUnwrap,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
#[unwrap(ref)]
#[try_unwrap(ref)]
pub enum Module {
	/// A library module.
	Library(Library),

	/// A document module.
	Document(Document),

	/// A normal module.
	Normal(Normal),
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "camelCase")]
pub struct Library {
	/// The module's path.
	pub path: crate::Path,
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "camelCase")]
pub struct Normal {
	/// The module's lock.
	pub lock: tg::lock::Id,

	/// The module's package.
	pub package: tg::directory::Id,

	/// The module's path.
	pub path: crate::Path,
}

impl Module {
	/// Return the source of this module.
	#[must_use]
	pub fn source(&self) -> (Option<tg::directory::Id>, tg::Path) {
		match self {
			Self::Library(library) => (None, library.path.clone()),
			Self::Document(document) => (
				None,
				document
					.package_path
					.join(&document.path)
					.try_into()
					.unwrap(),
			),
			Self::Normal(normal) => (Some(normal.package.clone()), normal.path.clone()),
		}
	}
}

impl From<Module> for Url {
	fn from(value: Module) -> Self {
		// Serialize and encode the module.
		let data =
			data_encoding::HEXLOWER.encode(serde_json::to_string(&value).unwrap().as_bytes());

		let path = match value {
			Module::Library(library) => library.path.to_string(),
			Module::Document(document) => {
				format!("{}/{}", document.package_path.display(), document.path)
			},
			Module::Normal(normal) => normal.path.to_string(),
		};

		// Create the URL.
		format!("tg://{data}/{path}").parse().unwrap()
	}
}

impl TryFrom<Url> for Module {
	type Error = tg::Error;

	fn try_from(url: Url) -> tg::Result<Self, Self::Error> {
		// Ensure the scheme is "tg".
		if url.scheme() != "tg" {
			return Err(error!(%url, "the URL has an invalid scheme"));
		}

		// Get the domain.
		let data = url
			.domain()
			.ok_or_else(|| error!(%url, "the URL must have a domain"))?;

		// Decode the data.
		let data = data_encoding::HEXLOWER
			.decode(data.as_bytes())
			.map_err(|source| error!(!source, "failed to deserialize the path"))?;

		// Deserialize the data.
		let module = serde_json::from_slice(&data)
			.map_err(|source| error!(!source, "failed to deserialize the module"))?;

		Ok(module)
	}
}

impl std::fmt::Display for Module {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let (package, path) = self.source();
		if let Some(package) = package {
			write!(f, "{package}/")?;
		}
		write!(f, "{path}")
	}
}

impl std::str::FromStr for Module {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		let url: Url = s
			.parse()
			.map_err(|source| error!(!source, "failed to parse the URL"))?;
		let module = url.try_into()?;
		Ok(module)
	}
}

impl From<Module> for String {
	fn from(value: Module) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Module {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self, Self::Error> {
		value.parse()
	}
}
