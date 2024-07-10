use crate as tg;
use either::Either;
use url::Url;

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Module {
	pub kind: Kind,

	#[serde(with = "either::serde_untagged")]
	pub object: Either<tg::Path, tg::object::Id>,
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
	Object,
	Artifact,
	Blob,
	Leaf,
	Branch,
	Directory,
	File,
	Symlink,
	Package,
	Target,
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
			Kind::Object => write!(f, "object"),
			Kind::Artifact => write!(f, "artifact"),
			Kind::Blob => write!(f, "blob"),
			Kind::Leaf => write!(f, "leaf"),
			Kind::Branch => write!(f, "branch"),
			Kind::Directory => write!(f, "directory"),
			Kind::File => write!(f, "file"),
			Kind::Symlink => write!(f, "symlink"),
			Kind::Package => write!(f, "package"),
			Kind::Target => write!(f, "target"),
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
			"object" => Ok(Kind::Object),
			"artifact" => Ok(Kind::Artifact),
			"blob" => Ok(Kind::Blob),
			"leaf" => Ok(Kind::Leaf),
			"branch" => Ok(Kind::Branch),
			"directory" => Ok(Kind::Directory),
			"file" => Ok(Kind::File),
			"symlink" => Ok(Kind::Symlink),
			"package" => Ok(Kind::Package),
			"target" => Ok(Kind::Target),
			_ => Err(tg::error!(%kind = s, "invalid kind")),
		}
	}
}
