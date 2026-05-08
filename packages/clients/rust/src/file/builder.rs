use {crate::prelude::*, std::collections::BTreeMap};

#[derive(Clone, Debug, Default)]
pub struct Builder {
	contents: Option<tg::Blob>,
	dependencies: BTreeMap<tg::Reference, Option<tg::file::Dependency>>,
	executable: bool,
	module: Option<tg::module::Kind>,
}

impl Builder {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn contents(mut self, contents: impl Into<tg::Blob>) -> Self {
		self.contents = Some(contents.into());
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

	pub fn build(self) -> tg::Result<tg::File> {
		let contents = self
			.contents
			.ok_or_else(|| tg::error!("cannot create a file without contents"))?;
		Ok(tg::File::with_object(tg::file::Object::Node(
			tg::file::object::Node {
				contents,
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
			},
		)))
	}
}
