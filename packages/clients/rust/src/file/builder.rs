use {crate::prelude::*, std::collections::BTreeMap};

pub struct Builder {
	contents: tg::Blob,
	dependencies: BTreeMap<tg::Reference, Option<tg::file::Dependency>>,
	executable: bool,
	module: Option<tg::module::Kind>,
}

impl Builder {
	#[must_use]
	pub fn new(contents: impl Into<tg::Blob>) -> Self {
		Self {
			contents: contents.into(),
			dependencies: BTreeMap::new(),
			executable: false,
			module: None,
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
		dependencies: impl IntoIterator<Item = (tg::Reference, Option<tg::file::Dependency>)>,
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
	pub fn module(mut self, module: Option<impl Into<tg::module::Kind>>) -> Self {
		self.module = module.map(std::convert::Into::into);
		self
	}

	#[must_use]
	pub fn build(self) -> tg::File {
		tg::File::with_object(tg::file::Object::Node(tg::file::object::Node {
			contents: self.contents,
			dependencies: self
				.dependencies
				.into_iter()
				.map(|(reference, option)| {
					(
						reference,
						option.map(|dependency| {
							tg::graph::Dependency(
								dependency.0.map(|item| item.map(tg::graph::Edge::Object)),
							)
						}),
					)
				})
				.collect(),
			executable: self.executable,
			module: self.module,
		}))
	}
}
