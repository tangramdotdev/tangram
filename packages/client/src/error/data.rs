use {
	crate::prelude::*,
	byteorder::{ReadBytesExt as _, WriteBytesExt as _},
	bytes::Bytes,
	std::{
		collections::{BTreeMap, BTreeSet},
		path::PathBuf,
	},
	tangram_either::Either,
};

/// An error.
#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Error {
	/// The error code.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub code: Option<tg::error::Code>,

	/// Diagnostics associated with the error.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub diagnostics: Option<Vec<tg::diagnostic::Data>>,

	/// The location where the error occurred.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	/// The error's message.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "Option::is_none")]
	pub message: Option<String>,

	/// The error's source.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 4, default, skip_serializing_if = "Option::is_none")]
	pub source: Option<tg::Referent<Either<Box<tg::error::Data>, tg::error::Id>>>,

	/// A stack trace associated with the error.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 5, default, skip_serializing_if = "Option::is_none")]
	pub stack: Option<Vec<Location>>,

	/// Values associated with the error.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	#[tangram_serialize(id = 6, default, skip_serializing_if = "BTreeMap::is_empty")]
	pub values: BTreeMap<String, String>,
}

/// An error location.
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Location {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub symbol: Option<String>,
	#[tangram_serialize(id = 1)]
	pub file: File,
	#[tangram_serialize(id = 2)]
	pub range: tg::Range,
}

/// An error location's source.
#[derive(
	Clone,
	Debug,
	derive_more::TryUnwrap,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
#[try_unwrap(ref)]
pub enum File {
	#[tangram_serialize(id = 0)]
	Internal(PathBuf),
	#[tangram_serialize(id = 1)]
	Module(tg::module::Data),
}

impl Error {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.write_u8(0).unwrap();
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let mut reader = std::io::Cursor::new(bytes.as_ref());
		let format = reader
			.read_u8()
			.map_err(|source| tg::error!(!source, "failed to read the format"))?;
		let error = match format {
			0 => tangram_serialize::from_reader(&mut reader)
				.map_err(|source| tg::error!(!source, "failed to deserialize the data")),
			b'{' => serde_json::from_slice(bytes.as_ref())
				.map_err(|source| tg::error!(!source, "failed to deserialize the data")),
			_ => Err(tg::error!("invalid format")),
		}?;
		Ok(error)
	}

	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		if let Some(diagnostics) = &self.diagnostics {
			for diagnostic in diagnostics {
				diagnostic.children(children);
			}
		}
		if let Some(location) = &self.location {
			location.children(children);
		}
		if let Some(stack) = &self.stack {
			for location in stack {
				location.children(children);
			}
		}
		if let Some(source) = &self.source {
			match &source.item {
				Either::Left(data) => data.children(children),
				Either::Right(id) => {
					children.insert(id.clone().into());
				},
			}
		}
	}

	pub fn remove_internal_locations(&mut self) {
		let mut stack = vec![self];
		while let Some(error) = stack.pop() {
			if let Some(location) = &error.location
				&& matches!(location.file, File::Internal(_))
			{
				error.location = None;
			}
			if let Some(stack_locations) = &mut error.stack {
				stack_locations.retain(|location| !matches!(location.file, File::Internal(_)));
			}
			if let Some(source) = &mut error.source
				&& let Either::Left(data) = &mut source.item
			{
				stack.push(&mut **data);
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
