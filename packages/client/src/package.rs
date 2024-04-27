pub use self::{get::*, module::*};

pub mod check;
pub mod doc;
pub mod format;
pub mod get;
pub mod list;
pub mod module;
pub mod outdated;
pub mod publish;
pub mod versions;
pub mod yank;

/// The possible file names of the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] =
	&["tangram.js", "tangram.tg.js", "tangram.tg.ts", "tangram.ts"];

/// The file name of the lockfile in a package.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Metadata {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub version: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub description: Option<String>,
}
