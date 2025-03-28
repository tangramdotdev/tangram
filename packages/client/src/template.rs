use crate as tg;
use futures::{TryStreamExt as _, stream::FuturesOrdered};
use itertools::Itertools as _;
use std::{borrow::Cow, collections::BTreeSet};

pub use self::component::Component;

#[derive(Clone, Debug, Default)]
pub struct Template {
	pub components: Vec<Component>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub components: Vec<component::Data>,
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

	pub fn artifacts(&self) -> impl Iterator<Item = &tg::Artifact> {
		self.components
			.iter()
			.filter_map(|component| match component {
				Component::String(_) => None,
				Component::Artifact(artifact) => Some(artifact),
			})
	}

	#[must_use]
	pub fn objects(&self) -> Vec<tg::object::Handle> {
		self.artifacts()
			.map(|artifact| artifact.clone().into())
			.collect()
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		let components = self
			.components
			.iter()
			.map(|component| component.data(handle))
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		Ok(Data { components })
	}

	pub fn try_render_sync<'a, F>(&'a self, mut f: F) -> tg::Result<String>
	where
		F: (FnMut(&'a Component) -> tg::Result<Cow<'a, str>>) + 'a,
	{
		let mut string = String::new();
		for component in &self.components {
			string.push_str(&f(component)?);
		}
		Ok(string)
	}

	pub async fn try_render<'a, F, Fut>(&'a self, f: F) -> tg::Result<String>
	where
		F: (FnMut(&'a Component) -> Fut) + 'a,
		Fut: Future<Output = tg::Result<String>> + 'a,
	{
		Ok(self
			.components
			.iter()
			.map(f)
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?
			.join(""))
	}

	pub fn unrender(prefix: &str, string: &str) -> tg::Result<Self> {
		let data = Data::unrender(prefix, string)?;
		let components = data.components.into_iter().map(|data| match data {
			component::Data::Artifact(id) => Component::Artifact(tg::Artifact::with_id(id)),
			component::Data::String(string) => Component::String(string),
		});
		Ok(Self::with_components(components))
	}
}

impl Data {
	#[must_use]
	pub fn with_components(components: impl IntoIterator<Item = component::Data>) -> Self {
		let components = components.into_iter().collect();
		Self { components }
	}

	#[must_use]
	pub fn components(&self) -> &[component::Data] {
		&self.components
	}

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		self.components
			.iter()
			.filter_map(|component| match component {
				component::Data::String(_) => None,
				component::Data::Artifact(id) => Some(id.clone().into()),
			})
			.collect()
	}

	pub fn try_render<'a, F>(&'a self, mut f: F) -> tg::Result<String>
	where
		F: (FnMut(&'a self::component::Data) -> tg::Result<Cow<'a, str>>) + 'a,
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
		F: (FnMut(&'a self::component::Data) -> Cow<'a, str>) + 'a,
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
				components.push(component::Data::String(
					string[i..match_.start()].to_owned(),
				));
			}

			// Get and parse the ID.
			let id = captures.get(1).unwrap();
			let id = id.as_str().parse().unwrap();

			// Add an artifact component.
			components.push(component::Data::Artifact(id));

			// Advance the cursor to the end of the match.
			i = match_.end();
		}

		// Add the remaining text as a string component.
		if i < string.len() {
			components.push(component::Data::String(string[i..].to_owned()));
		}

		// Create the template.
		Ok(Self { components })
	}
}

impl TryFrom<Data> for Template {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let components = data
			.components
			.into_iter()
			.map(TryInto::try_into)
			.try_collect()?;
		Ok(Self { components })
	}
}

impl From<Component> for Template {
	fn from(value: Component) -> Self {
		vec![value].into()
	}
}

impl From<Vec<Component>> for Template {
	fn from(value: Vec<Component>) -> Self {
		Self { components: value }
	}
}

impl FromIterator<Component> for Template {
	fn from_iter<I: IntoIterator<Item = Component>>(value: I) -> Self {
		Self {
			components: value.into_iter().collect(),
		}
	}
}

impl From<String> for Template {
	fn from(value: String) -> Self {
		vec![Component::String(value)].into()
	}
}

impl From<&str> for Template {
	fn from(value: &str) -> Self {
		value.to_owned().into()
	}
}

pub mod component {
	use crate as tg;

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
	pub enum Component {
		String(String),
		Artifact(tg::Artifact),
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
	)]
	#[try_unwrap(ref)]
	#[unwrap(ref)]
	#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
	pub enum Data {
		String(String),
		Artifact(tg::artifact::Id),
	}

	impl Component {
		pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
		where
			H: tg::Handle,
		{
			match self {
				Self::String(string) => Ok(Data::String(string.clone())),
				Self::Artifact(artifact) => Ok(Data::Artifact(artifact.id(handle).await?)),
			}
		}
	}

	impl TryFrom<Data> for Component {
		type Error = tg::Error;

		fn try_from(data: Data) -> tg::Result<Self, Self::Error> {
			Ok(match data {
				Data::String(string) => Self::String(string),
				Data::Artifact(id) => Self::Artifact(tg::Artifact::with_id(id)),
			})
		}
	}
}

impl std::fmt::Display for Template {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.template(self)?;
		Ok(())
	}
}

impl From<tg::Directory> for Component {
	fn from(value: tg::Directory) -> Self {
		Self::Artifact(value.into())
	}
}

impl From<tg::File> for Component {
	fn from(value: tg::File) -> Self {
		Self::Artifact(value.into())
	}
}

impl From<tg::Symlink> for Component {
	fn from(value: tg::Symlink) -> Self {
		Self::Artifact(value.into())
	}
}

impl From<tg::directory::Id> for component::Data {
	fn from(value: tg::directory::Id) -> Self {
		Self::Artifact(value.into())
	}
}

impl From<tg::file::Id> for component::Data {
	fn from(value: tg::file::Id) -> Self {
		Self::Artifact(value.into())
	}
}

impl From<tg::symlink::Id> for component::Data {
	fn from(value: tg::symlink::Id) -> Self {
		Self::Artifact(value.into())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn unrender() {
		let id = "dir_010000000000000000000000000000000000000000000000000000"
			.parse()
			.unwrap();
		let string = format!("foo /path/to/.tangram/artifacts/{id} bar");
		let template = tg::Template::unrender("/path/to/.tangram/artifacts", &string).unwrap();

		let left = template.components().first().unwrap().unwrap_string_ref();
		let right = "foo ";
		assert_eq!(left, right);

		let left = template
			.components()
			.get(1)
			.unwrap()
			.unwrap_artifact_ref()
			.unwrap_directory_ref()
			.state()
			.read()
			.unwrap()
			.id()
			.cloned()
			.unwrap();
		let right = id;
		assert_eq!(left, right);

		let left = template.components().get(2).unwrap().unwrap_string_ref();
		let right = " bar";
		assert_eq!(left, right);
	}
}
