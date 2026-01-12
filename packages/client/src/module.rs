use {
	crate::prelude::*,
	std::path::{Path, PathBuf},
};

pub use self::{data::Module as Data, import::Import};

pub mod data;
pub mod import;
pub mod load;
pub mod resolve;

#[derive(Clone, Debug)]
pub struct Module {
	pub kind: Kind,
	pub referent: tg::Referent<Item>,
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
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub enum Kind {
	Js,
	Ts,
	Dts,
	Py,
	Pyi,
	Object,
	Artifact,
	Blob,
	Directory,
	File,
	Symlink,
	Graph,
	Command,
	Error,
}

#[derive(
	Clone,
	Debug,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Item {
	Edge(tg::graph::Edge<tg::Object>),
	Path(PathBuf),
}

impl Module {
	#[must_use]
	pub fn children(&self) -> Vec<tg::object::Handle> {
		match &self.referent.item {
			Item::Edge(edge) => edge.children(),
			Item::Path(_) => vec![],
		}
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		let kind = self.kind;
		let referent = self.referent.clone().map(|item| match item {
			Item::Edge(edge) => tg::module::data::Item::Edge(edge.to_data()),
			Item::Path(path) => tg::module::data::Item::Path(path),
		});
		tg::module::Data { kind, referent }
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let kind = data.kind;
		let referent = data.referent.try_map(|item| {
			let item = match item {
				tg::module::data::Item::Edge(edge) => {
					let edge = tg::graph::Edge::try_from_data(edge)?;
					tg::module::Item::Edge(edge)
				},
				tg::module::data::Item::Path(path) => tg::module::Item::Path(path),
			};
			Ok::<_, tg::Error>(item)
		})?;
		let module = Self { kind, referent };
		Ok(module)
	}
}

impl TryFrom<tg::module::Data> for Module {
	type Error = tg::Error;

	fn try_from(value: tg::module::Data) -> Result<Self, Self::Error> {
		Self::try_from_data(value)
	}
}

impl std::fmt::Display for Module {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.module(self)?;
		Ok(())
	}
}

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Js => write!(f, "js"),
			Self::Ts => write!(f, "ts"),
			Self::Dts => write!(f, "dts"),
			Self::Py => write!(f, "py"),
			Self::Pyi => write!(f, "pyi"),
			Self::Object => write!(f, "object"),
			Self::Artifact => write!(f, "artifact"),
			Self::Blob => write!(f, "blob"),
			Self::Directory => write!(f, "directory"),
			Self::File => write!(f, "file"),
			Self::Symlink => write!(f, "symlink"),
			Self::Graph => write!(f, "graph"),
			Self::Command => write!(f, "command"),
			Self::Error => write!(f, "error"),
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
			"py" => Ok(Self::Py),
			"pyi" => Ok(Self::Pyi),
			"object" => Ok(Self::Object),
			"artifact" => Ok(Self::Artifact),
			"blob" => Ok(Self::Blob),
			"directory" => Ok(Self::Directory),
			"file" => Ok(Self::File),
			"symlink" => Ok(Self::Symlink),
			"graph" => Ok(Self::Graph),
			"command" => Ok(Self::Command),
			"error" => Ok(Self::Error),
			_ => Err(tg::error!(kind = %s, "invalid kind")),
		}
	}
}

/// The possible file names for the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] = &["tangram.js", "tangram.ts", "tangram.py"];

/// The file name of a lockfile.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

pub fn module_kind_for_path(path: impl AsRef<Path>) -> tg::Result<tg::module::Kind> {
	let path = path.as_ref();
	let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
		return Err(tg::error!(path = %path.display(), "invalid path"));
	};
	if name.ends_with(".d.ts") {
		Ok(tg::module::Kind::Dts)
	} else if name == "tangram.js" || name.ends_with(".tg.js") {
		Ok(tg::module::Kind::Js)
	} else if name == "tangram.ts" || name.ends_with(".tg.ts") {
		Ok(tg::module::Kind::Ts)
	} else if name == "tangram.py" || name.ends_with(".tg.py") || name.ends_with(".py") {
		Ok(tg::module::Kind::Py)
	} else if name.ends_with(".pyi") {
		Ok(tg::module::Kind::Pyi)
	} else {
		Err(tg::error!(path = %path.display(), "unknown or missing file extension"))
	}
}

/// Get the host for a module kind.
#[must_use]
pub fn host_for_module_kind(kind: tg::module::Kind) -> &'static str {
	match kind {
		tg::module::Kind::Js | tg::module::Kind::Ts | tg::module::Kind::Dts => "js",
		tg::module::Kind::Py | tg::module::Kind::Pyi => "python",
		_ => "js",
	}
}

#[must_use]
pub fn is_module_path(path: &Path) -> bool {
	let Some(name) = path.file_name() else {
		return false;
	};
	let Some(name) = name.to_str() else {
		return false;
	};
	tg::module::ROOT_MODULE_FILE_NAMES.contains(&name)
		|| name.ends_with(".tg.js")
		|| name.ends_with(".tg.ts")
		|| name.ends_with(".tg.py")
		|| name.ends_with(".py")
}

#[must_use]
pub fn is_root_module_path(path: &Path) -> bool {
	let Some(name) = path.file_name() else {
		return false;
	};
	let Some(name) = name.to_str() else {
		return false;
	};
	tg::module::ROOT_MODULE_FILE_NAMES.contains(&name)
}

#[must_use]
pub fn is_non_root_module_path(path: &Path) -> bool {
	let Some(name) = path.file_name() else {
		return false;
	};
	let Some(name) = name.to_str() else {
		return false;
	};
	!tg::module::ROOT_MODULE_FILE_NAMES.contains(&name)
		&& (name.ends_with(".tg.js")
			|| name.ends_with(".tg.ts")
			|| name.ends_with(".tg.py")
			|| name.ends_with(".py"))
}

pub async fn try_get_root_module_file_name<H>(
	handle: &H,
	package: tg::Either<&tg::Directory, &Path>,
) -> tg::Result<Option<&'static str>>
where
	H: tg::Handle,
{
	let mut name = None;
	for name_ in tg::module::ROOT_MODULE_FILE_NAMES {
		let exists = match package {
			tg::Either::Left(directory) => directory.try_get_entry(handle, name_).await?.is_some(),
			tg::Either::Right(path) => tokio::fs::try_exists(path.join(*name_)).await.map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to get the metadata"),
			)?,
		};
		if exists {
			if name.is_some() {
				return Err(tg::error!("package contains multiple root modules"));
			}
			name = Some(*name_);
		}
	}
	Ok(name)
}

pub fn try_get_root_module_file_name_sync(path: &Path) -> tg::Result<Option<&'static str>> {
	let mut name = None;
	for name_ in tg::module::ROOT_MODULE_FILE_NAMES {
		let exists = path.join(name_).try_exists().map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to get the metadata"),
		)?;
		if exists {
			if name.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			name = Some(*name_);
		}
	}
	Ok(name)
}
