use std::path::PathBuf;

/// A document.
#[derive(
	Clone, PartialOrd, Ord, PartialEq, Eq, Hash, Debug, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "camelCase")]
pub struct Document {
	/// The path to the package.
	pub package_path: PathBuf,

	/// The module's path.
	pub path: crate::Path,
}

/// A document's state.
#[derive(Clone, Debug)]
pub enum State {
	/// A closed document.
	Closed(Closed),

	/// An opened document.
	Opened(Opened),
}

/// A closed document.
#[derive(Clone, Debug)]
pub struct Closed {
	/// The document's version.
	pub version: i32,

	/// The document's last modified time.
	pub modified: std::time::SystemTime,
}

/// An opened document.
#[derive(Clone, Debug)]
pub struct Opened {
	/// The document's version.
	pub version: i32,

	/// The document's text.
	pub text: String,
}

impl Document {
	/// Get the document's path.
	#[must_use]
	pub fn path(&self) -> PathBuf {
		self.package_path.join(self.path.to_string())
	}
}
