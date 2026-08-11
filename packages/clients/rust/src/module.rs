use {
	crate::prelude::*,
	std::path::{Path, PathBuf},
};

pub mod data;
pub mod import;
pub mod load;
pub mod location;
pub mod resolve;

pub use self::{data::Module as Data, import::Import, location::Location};

#[derive(Clone, Debug)]
pub struct Module {
	pub kind: Kind,
	pub referent: tg::Referent<Source>,
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
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum Kind {
	#[tangram_serialize(id = 0)]
	Js,

	#[tangram_serialize(id = 1)]
	Ts,

	#[tangram_serialize(id = 2)]
	Dts,

	#[tangram_serialize(id = 3)]
	Object,

	#[tangram_serialize(id = 4)]
	Artifact,

	#[tangram_serialize(id = 5)]
	Blob,

	#[tangram_serialize(id = 6)]
	Directory,

	#[tangram_serialize(id = 7)]
	File,

	#[tangram_serialize(id = 8)]
	Symlink,

	#[tangram_serialize(id = 9)]
	Graph,

	#[tangram_serialize(id = 10)]
	Command,

	#[tangram_serialize(id = 11)]
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
pub enum Source {
	Edge(tg::graph::Edge<tg::Object>),
	Path(PathBuf),
}

impl Module {
	#[must_use]
	pub fn children(&self) -> Vec<tg::object::Handle> {
		match &self.referent.node {
			Source::Edge(edge) => edge.children(),
			Source::Path(_) => vec![],
		}
	}

	#[must_use]
	pub fn without_token(&self) -> Self {
		let mut module = self.clone();
		module.referent = module.referent.without_token();

		module
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		let kind = self.kind;
		let referent = self.referent.clone().map(|source| match source {
			Source::Edge(edge) => tg::module::data::Source::Edge(edge.to_data()),
			Source::Path(path) => tg::module::data::Source::Path(path),
		});
		tg::module::Data { kind, referent }
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let kind = data.kind;
		let referent = data.referent.try_map(|source| {
			let source = match source {
				tg::module::data::Source::Edge(edge) => {
					let edge = tg::graph::Edge::try_from_data(edge)?;
					tg::module::Source::Edge(edge)
				},
				tg::module::data::Source::Path(path) => tg::module::Source::Path(path),
			};
			Ok::<_, tg::Error>(source)
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

/// The possible file names for the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] = &["tangram.js", "tangram.ts"];

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
	} else {
		Err(tg::error!(path = %path.display(), "unknown or missing file extension"))
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
		&& (name.ends_with(".tg.js") || name.ends_with(".tg.ts"))
}

pub async fn try_get_root_module_file_name(
	package: tg::Either<&tg::Directory, &Path>,
) -> tg::Result<Option<&'static str>> {
	let handle = tg::handle()?;
	try_get_root_module_file_name_with_handle(handle, package).await
}

pub async fn try_get_root_module_file_name_with_handle<H>(
	handle: &H,
	package: tg::Either<&tg::Directory, &Path>,
) -> tg::Result<Option<&'static str>>
where
	H: tg::Handle,
{
	let mut name = None;
	for name_ in tg::module::ROOT_MODULE_FILE_NAMES {
		let exists = match package {
			tg::Either::Left(directory) => directory
				.try_get_entry_with_handle(handle, name_)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the entry"))?
				.is_some(),
			tg::Either::Right(path) => tokio::fs::try_exists(path.join(*name_)).await.map_err(
				|error| tg::error!(!error, path = %path.display(), "failed to get the metadata"),
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
			|error| tg::error!(!error, path = %path.display(), "failed to get the metadata"),
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
