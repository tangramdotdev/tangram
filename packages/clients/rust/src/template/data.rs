use {
	crate::prelude::*,
	serde_with::DisplayFromStr,
	std::{borrow::Cow, collections::BTreeSet},
};

#[derive(
	Clone,
	Debug,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Template {
	#[tangram_serialize(id = 0)]
	pub components: Vec<Component>,
}

#[derive(
	Clone,
	Debug,
	PartialEq,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Component {
	#[tangram_serialize(id = 0)]
	String(String),
	#[tangram_serialize(id = 1)]
	Artifact(#[serde(with = "serde_with::As::<DisplayFromStr>")] tg::Referent<tg::artifact::Id>),
	#[tangram_serialize(id = 2)]
	Placeholder(tg::placeholder::Data),
}

impl Template {
	#[must_use]
	pub fn with_components(components: impl IntoIterator<Item = Component>) -> Self {
		let components = components.into_iter().collect();
		Self { components }
	}

	#[must_use]
	pub fn components(&self) -> &[Component] {
		&self.components
	}

	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		for component in &self.components {
			if let Component::Artifact(artifact) = component {
				children.insert(artifact.node.clone().into());
			}
		}
	}

	pub fn children_with_tokens(&self, children: &mut Vec<tg::Referent<tg::object::Id>>) {
		for component in &self.components {
			if let Component::Artifact(artifact) = component {
				let object = artifact.clone().map(Into::into);
				children.push(object);
			}
		}
	}

	#[must_use]
	pub fn without_location_and_tokens(mut self) -> Self {
		for component in &mut self.components {
			if let Component::Artifact(artifact) = component {
				artifact.options.clear_location_and_tokens();
			}
		}

		self
	}

	pub fn try_render<'a, F>(&'a self, mut f: F) -> tg::Result<String>
	where
		F: (FnMut(&'a Component) -> tg::Result<Cow<'a, str>>) + 'a,
	{
		let mut string = String::new();
		for component in &self.components {
			let component = f(component)?;
			string.push_str(&component);
		}
		Ok(string)
	}

	pub fn render<'a, F>(&'a self, mut f: F) -> String
	where
		F: (FnMut(&'a Component) -> Cow<'a, str>) + 'a,
	{
		let mut string = String::new();
		for component in &self.components {
			let component = f(component);
			string.push_str(&component);
		}
		string
	}

	pub fn unrender(prefix: &str, string: &str) -> tg::Result<Self> {
		// Create the regex.
		let prefix = regex::escape(prefix);
		let regex =
			format!(r"{prefix}/((?:dir_|fil_|sym_)01[0123456789abcdefghjkmnpqrstvwxyz]{{52}})");
		let regex = regex::Regex::new(&regex).unwrap();

		let mut i = 0;
		let mut components = Vec::new();
		for captures in regex.captures_iter(string) {
			// Add the text leading up to the capture as a string component.
			let match_ = captures.get(0).unwrap();
			if match_.start() > i {
				components.push(Component::String(string[i..match_.start()].to_owned()));
			}

			// Get and parse the ID.
			let id = captures.get(1).unwrap();
			let id: tg::artifact::Id = id.as_str().parse().unwrap();

			// Add an artifact component.
			components.push(Component::Artifact(tg::Referent::with_node(id)));

			// Advance the cursor to the end of the match.
			i = match_.end();
		}

		// Add the remaining text as a string component.
		if i < string.len() {
			components.push(Component::String(string[i..].to_owned()));
		}

		// Create the template.
		Ok(Self { components })
	}
}

impl From<tg::artifact::Id> for Component {
	fn from(value: tg::artifact::Id) -> Self {
		Self::Artifact(tg::Referent::with_node(value))
	}
}

impl From<tg::directory::Id> for Component {
	fn from(value: tg::directory::Id) -> Self {
		Self::Artifact(tg::Referent::with_node(value.into()))
	}
}

impl From<tg::file::Id> for Component {
	fn from(value: tg::file::Id) -> Self {
		Self::Artifact(tg::Referent::with_node(value.into()))
	}
}

impl From<tg::symlink::Id> for Component {
	fn from(value: tg::symlink::Id) -> Self {
		Self::Artifact(tg::Referent::with_node(value.into()))
	}
}
