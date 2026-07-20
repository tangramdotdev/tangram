use {
	crate::prelude::*,
	byteorder::WriteBytesExt as _,
	bytes::Bytes,
	std::{
		collections::{BTreeMap, BTreeSet},
		path::PathBuf,
	},
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
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub code: Option<tg::error::Code>,

	/// Diagnostics associated with the error.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub diagnostics: Option<Vec<tg::diagnostic::Data>>,

	/// The location where the error occurred.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	/// The error's message.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Option::is_none")]
	pub message: Option<String>,

	/// The error's source.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "Option::is_none")]
	pub source: Option<tg::Referent<tg::Either<Box<tg::error::Data>, tg::error::Id>>>,

	/// A stack trace associated with the error.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub stack: Option<Vec<Location>>,

	/// Values associated with the error.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "BTreeMap::is_empty")]
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
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
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
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
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
			.map_err(|error| tg::error!(!error, "failed to serialize the data"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let bytes = bytes.as_ref();
		if bytes.is_empty() {
			return Err(tg::error!("missing format byte"));
		}
		let format = bytes[0];
		let error = match format {
			0 => tangram_serialize::from_slice(&bytes[1..])
				.map_err(|error| tg::error!(!error, "failed to deserialize the data")),
			b'{' => serde_json::from_slice(bytes)
				.map_err(|error| tg::error!(!error, "failed to deserialize the data")),
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
				tg::Either::Left(data) => data.children(children),
				tg::Either::Right(id) => {
					children.insert(id.clone().into());
				},
			}
		}
	}

	#[must_use]
	pub fn without_tokens(mut self) -> Self {
		self.diagnostics = self.diagnostics.map(|diagnostics| {
			diagnostics
				.into_iter()
				.map(tg::diagnostic::Data::without_tokens)
				.collect()
		});
		self.location = self.location.map(Location::without_tokens);
		self.source = self.source.map(|mut source| {
			source.options.token.take();
			source.item = match source.item {
				tg::Either::Left(error) => tg::Either::Left(Box::new((*error).without_tokens())),
				tg::Either::Right(id) => tg::Either::Right(id),
			};
			source
		});
		self.stack = self
			.stack
			.map(|stack| stack.into_iter().map(Location::without_tokens).collect());

		self
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
				&& let tg::Either::Left(data) = &mut source.item
			{
				stack.push(data);
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

	#[must_use]
	pub fn without_tokens(mut self) -> Self {
		self.file = match self.file {
			File::Internal(path) => File::Internal(path),
			File::Module(module) => File::Module(module.without_tokens()),
		};

		self
	}
}
