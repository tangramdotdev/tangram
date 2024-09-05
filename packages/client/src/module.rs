use crate as tg;
use tangram_either::Either;
use tangram_uri as uri;

/// The possible file names for the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] = &["tangram.js", "tangram.ts"];

/// The file name of the lockfile in a package.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub struct Module {
	uri: uri::Reference,
	path: Path,
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Path {
	pub kind: Kind,
	pub object: Option<Either<tg::object::Id, tg::Path>>,
	pub path: Option<tg::Path>,
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
	Blob,
	Artifact,
	Leaf,
	Branch,
	Directory,
	File,
	Symlink,
	Graph,
	Target,
}

impl Module {
	#[must_use]
	pub fn new(
		kind: Kind,
		object: Option<Either<tg::object::Id, tg::Path>>,
		path: Option<tg::Path>,
	) -> Self {
		let path = Path { kind, object, path };
		let json = serde_json::to_string(&path).unwrap();
		let hex = data_encoding::HEXLOWER.encode(json.as_bytes());
		let string = format!("tg:{hex}");
		let uri = string.parse().unwrap();
		Self { uri, path }
	}

	pub async fn with_package<H>(
		handle: &H,
		package: Either<tg::Directory, tg::Path>,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		Self::try_with_package(handle, package)
			.await?
			.ok_or_else(|| tg::error!("the package does not contain a root module"))
	}

	pub async fn try_with_package<H>(
		handle: &H,
		package: Either<tg::Directory, tg::Path>,
	) -> tg::Result<Option<Self>>
	where
		H: tg::Handle,
	{
		let mut name = None;
		for n in ROOT_MODULE_FILE_NAMES {
			let exists = match &package {
				Either::Left(package) => package.try_get_entry(handle, n).await?.is_some(),
				Either::Right(package) => tokio::fs::try_exists(package.clone().join(*n))
					.await
					.map_err(|source| tg::error!(!source, "failed to get the path metadata"))?,
			};
			if exists {
				if name.is_some() {
					return Err(tg::error!("package contains multiple root modules"));
				}
				name = Some(*n);
			}
		}
		let Some(name) = name else {
			return Ok(None);
		};
		let kind = if name.ends_with("js") {
			tg::module::Kind::Js
		} else if name.ends_with("ts") {
			tg::module::Kind::Ts
		} else {
			unreachable!()
		};
		let object = match package {
			Either::Left(package) => Either::Left(package.id(handle).await?.into()),
			Either::Right(package) => Either::Right(package),
		};
		let path = name.into();
		let module = Self::new(kind, Some(object), Some(path));
		Ok(Some(module))
	}

	#[must_use]
	pub fn uri(&self) -> &uri::Reference {
		&self.uri
	}

	#[must_use]
	pub fn as_str(&self) -> &str {
		self.uri.as_str()
	}

	#[must_use]
	pub fn kind(&self) -> Kind {
		self.path.kind
	}

	#[must_use]
	pub fn object(&self) -> Option<&Either<tg::object::Id, tg::Path>> {
		self.path.object.as_ref()
	}

	#[must_use]
	pub fn path(&self) -> Option<&tg::Path> {
		self.path.path.as_ref()
	}
}

impl std::fmt::Display for Module {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.uri)
	}
}

impl std::str::FromStr for Module {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		let reference: uri::Reference = s
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the reference"))?;

		// Ensure the scheme is "tg".
		if !matches!(reference.scheme(), Some("tg")) {
			return Err(tg::error!("the URI has an invalid scheme"));
		}

		// Get the path.
		let hex = reference.path();

		// Decode.
		let json = data_encoding::HEXLOWER
			.decode(hex.as_bytes())
			.map_err(|source| tg::error!(!source, "failed to deserialize the path"))?;

		// Deserialize.
		let path = serde_json::from_slice(&json)
			.map_err(|source| tg::error!(!source, "failed to deserialize the path"))?;

		Ok(Self {
			uri: reference,
			path,
		})
	}
}

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Js => write!(f, "js"),
			Self::Ts => write!(f, "ts"),
			Self::Dts => write!(f, "dts"),
			Self::Object => write!(f, "object"),
			Self::Artifact => write!(f, "artifact"),
			Self::Blob => write!(f, "blob"),
			Self::Leaf => write!(f, "leaf"),
			Self::Branch => write!(f, "branch"),
			Self::Directory => write!(f, "directory"),
			Self::File => write!(f, "file"),
			Self::Symlink => write!(f, "symlink"),
			Self::Graph => write!(f, "graph"),
			Self::Target => write!(f, "target"),
		}
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"js" => Ok(Self::Js),
			"ts" => Ok(Self::Ts),
			"dts" => Ok(Self::Dts),
			"object" => Ok(Self::Object),
			"artifact" => Ok(Self::Artifact),
			"blob" => Ok(Self::Blob),
			"leaf" => Ok(Self::Leaf),
			"branch" => Ok(Self::Branch),
			"directory" => Ok(Self::Directory),
			"file" => Ok(Self::File),
			"symlink" => Ok(Self::Symlink),
			"graph" => Ok(Self::Graph),
			"target" => Ok(Self::Target),
			_ => Err(tg::error!(%kind = s, "invalid kind")),
		}
	}
}
