use std::{
	collections::{BTreeMap, BTreeSet},
	path::PathBuf,
};

use crate as tg;

/// An error.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Error {
	/// The error code.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub code: Option<tg::error::Code>,

	/// The error's message.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub message: Option<String>,

	/// The location where the error occurred.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	/// A stack trace associated with the error.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stack: Option<Vec<Location>>,

	/// The error's source.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub source: Option<tg::Referent<Box<tg::error::Data>>>,

	/// Values associated with the error.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub values: BTreeMap<String, String>,
}

/// An error location.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Location {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub symbol: Option<String>,
	pub file: File,
	pub line: u32,
	pub column: u32,
}

/// An error location's source.
#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
#[try_unwrap(ref)]
pub enum File {
	Internal(PathBuf),
	Module(tg::module::Data),
}

impl Error {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		std::iter::empty()
			.chain(
				self.location
					.as_ref()
					.map(Location::children)
					.into_iter()
					.flatten(),
			)
			.collect()
	}
}

impl Location {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		self.file
			.try_unwrap_module_ref()
			.ok()
			.map(tg::module::Data::children)
			.into_iter()
			.flatten()
			.collect()
	}
}
