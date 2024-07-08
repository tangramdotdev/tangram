use crate as tg;
use either::Either;
use url::Url;

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Module {
	pub kind: Kind,

	#[serde(with = "either::serde_untagged")]
	pub package: Either<tg::Path, tg::package::Id>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Kind {
	Js,
	Ts,
	Dts,
	Artifact,
	Directory,
	File,
	Symlink,
}

impl From<Module> for Url {
	fn from(value: Module) -> Self {
		// Serialize and encode the module.
		let json = serde_json::to_string(&value).unwrap();
		let hex = data_encoding::HEXLOWER.encode(json.as_bytes());

		// Create the URL.
		format!("tg://{hex}").parse().unwrap()
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
		let hex = url
			.domain()
			.ok_or_else(|| tg::error!(%url, "the URL must have a domain"))?;

		// Decode.
		let json = data_encoding::HEXLOWER
			.decode(hex.as_bytes())
			.map_err(|source| tg::error!(!source, "failed to deserialize the path"))?;

		// Deserialize.
		let module = serde_json::from_slice(&json)
			.map_err(|source| tg::error!(!source, "failed to deserialize the module"))?;

		Ok(module)
	}
}

impl std::fmt::Display for Module {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", Url::from(self.clone()))
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

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Kind::Js => write!(f, "js"),
			Kind::Ts => write!(f, "ts"),
			Kind::Dts => write!(f, "dts"),
			Kind::Artifact => write!(f, "artifact"),
			Kind::Directory => write!(f, "directory"),
			Kind::File => write!(f, "file"),
			Kind::Symlink => write!(f, "symlink"),
		}
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"js" => Ok(Kind::Js),
			"ts" => Ok(Kind::Ts),
			"dts" => Ok(Kind::Dts),
			"artifact" => Ok(Kind::Artifact),
			"directory" => Ok(Kind::Directory),
			"file" => Ok(Kind::File),
			"symlink" => Ok(Kind::Symlink),
			_ => Err(tg::error!("invalid kind")),
		}
	}
}
