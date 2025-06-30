use crate as tg;
use std::collections::BTreeMap;

pub struct Builder {
	contents: tg::Blob,
	dependencies: BTreeMap<tg::Reference, tg::Referent<tg::Object>>,
	executable: bool,
}

impl Builder {
	#[must_use]
	pub fn new(contents: impl Into<tg::Blob>) -> Self {
		Self {
			contents: contents.into(),
			dependencies: BTreeMap::new(),
			executable: false,
		}
	}

	#[must_use]
	pub fn contents(mut self, contents: impl Into<tg::Blob>) -> Self {
		self.contents = contents.into();
		self
	}

	#[must_use]
	pub fn dependencies(
		mut self,
		dependencies: impl IntoIterator<Item = (tg::Reference, tg::Referent<tg::Object>)>,
	) -> Self {
		self.dependencies = dependencies.into_iter().collect();
		self
	}

	#[must_use]
	pub fn executable(mut self, executable: impl Into<bool>) -> Self {
		self.executable = executable.into();
		self
	}

	#[must_use]
	pub fn build(self) -> tg::File {
		tg::File::with_object(tg::file::Object::Node(tg::file::object::Node {
			contents: self.contents,
			dependencies: self.dependencies,
			executable: self.executable,
		}))
	}
}
