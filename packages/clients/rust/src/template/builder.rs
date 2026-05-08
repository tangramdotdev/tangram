use crate::prelude::*;

#[derive(Clone, Debug, Default)]
pub struct Builder {
	components: Vec<tg::template::Component>,
}

impl Builder {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn component(mut self, component: tg::template::Component) -> Self {
		self.components.push(component);
		self
	}

	#[must_use]
	pub fn components(
		mut self,
		components: impl IntoIterator<Item = tg::template::Component>,
	) -> Self {
		self.components.extend(components);
		self
	}

	#[must_use]
	pub fn string(self, string: impl Into<String>) -> Self {
		self.component(tg::template::Component::String(string.into()))
	}

	#[must_use]
	pub fn artifact(self, artifact: impl Into<tg::Artifact>) -> Self {
		self.component(tg::template::Component::Artifact(artifact.into()))
	}

	#[must_use]
	pub fn placeholder(self, placeholder: impl Into<tg::Placeholder>) -> Self {
		self.component(tg::template::Component::Placeholder(placeholder.into()))
	}

	#[must_use]
	pub fn build(self) -> tg::Template {
		tg::Template {
			components: self.components,
		}
	}
}
