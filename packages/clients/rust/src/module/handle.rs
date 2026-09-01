use {
	super::{Data, Kind},
	crate::prelude::*,
	std::path::PathBuf,
};

#[derive(Clone, Debug)]
pub struct Module {
	pub kind: Kind,
	pub referent: tg::Referent<Source>,
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

#[derive(Clone, Debug)]
pub struct Location {
	pub module: tg::Module,
	pub range: tg::Range,
}

impl Module {
	#[must_use]
	pub fn children(&self) -> Vec<tg::object::Handle> {
		let children = match &self.referent.node {
			Source::Edge(edge) => edge.children(),
			Source::Path(_) => vec![],
		};
		for child in &children {
			child.inherit_location(self.referent.options.location.as_ref());
			child.inherit_tokens(&self.referent.options.tokens);
		}
		children
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

impl Location {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		self.module.children()
	}

	#[must_use]
	pub fn to_data(&self) -> tg::module::data::Location {
		let module = self.module.to_data();
		let range = self.range;
		tg::module::data::Location { module, range }
	}

	pub fn try_from_data(data: tg::module::data::Location) -> tg::Result<Self> {
		let module = data.module.try_into()?;
		let range = data.range;
		Ok(Self { module, range })
	}
}

impl TryFrom<tg::module::data::Location> for Location {
	type Error = tg::Error;

	fn try_from(value: tg::module::data::Location) -> Result<Self, Self::Error> {
		Self::try_from_data(value)
	}
}

impl std::fmt::Display for Location {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.module)?;
		let start_line = self.range.start.line + 1;
		let start_character = self.range.start.character + 1;
		let end_line = self.range.end.line + 1;
		let end_character = self.range.end.character + 1;
		write!(
			f,
			":{start_line}:{start_character}-{end_line}:{end_character}"
		)?;
		Ok(())
	}
}
