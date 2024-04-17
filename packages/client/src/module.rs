use super::document::Document;
use crate as tg;
use itertools::Itertools as _;
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
	derive_more::Unwrap,
	derive_more::TryUnwrap,
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
	pub path: tg::Path,
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
	pub path: tg::Path,
}

impl Module {
	/// Return the source of this module.
	#[must_use]
	pub fn source(&self) -> (Option<tg::directory::Id>, tg::Path) {
		match self {
			Self::Library(library) => {
				let path = library.path.clone();
				(None, path)
			},
			Self::Document(document) => {
				let path = document
					.package_path
					.join(&document.path)
					.try_into()
					.unwrap();
				(None, path)
			},
			Self::Normal(normal) => {
				let package = Some(normal.package.clone());
				let path = normal.path.clone();
				(package, path)
			},
		}
	}
}

impl From<Module> for Url {
	fn from(value: Module) -> Self {
		// Serialize and encode the module.
		let data =
			data_encoding::HEXLOWER.encode(serde_json::to_string(&value).unwrap().as_bytes());

		let path = match value {
			Module::Library(library) => library.path.components().iter().skip(1).join("/"),
			Module::Document(document) => {
				let package_path = document.package_path.display();
				let path = document.path.components().iter().skip(1).join("/");
				format!("{package_path}/{path}")
			},
			Module::Normal(normal) => normal.path.components().iter().skip(1).join("/"),
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
			return Err(tg::error!(%url, "the URL has an invalid scheme"));
		}

		// Get the domain.
		let data = url
			.domain()
			.ok_or_else(|| tg::error!(%url, "the URL must have a domain"))?;

		// Decode the data.
		let data = data_encoding::HEXLOWER
			.decode(data.as_bytes())
			.map_err(|source| tg::error!(!source, "failed to deserialize the path"))?;

		// Deserialize the data.
		let module = serde_json::from_slice(&data)
			.map_err(|source| tg::error!(!source, "failed to deserialize the module"))?;

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
			.map_err(|source| tg::error!(!source, "failed to parse the URL"))?;
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
