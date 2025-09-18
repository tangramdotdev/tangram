use crate as tg;
use std::{
	collections::{BTreeMap, BTreeSet},
	path::PathBuf,
};

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
	pub range: tg::Range,
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
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		if let Some(location) = &self.location {
			location.children(children);
		}
		if let Some(stack) = &self.stack {
			for location in stack {
				location.children(children);
			}
		}
	}
}

impl Location {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		if let tg::error::data::File::Module(module) = &self.file {
			module.children(children);
		}
	}
}
