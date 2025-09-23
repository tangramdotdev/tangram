use {
	crate as tg,
	futures::{TryStreamExt as _, stream::FuturesOrdered},
	std::{borrow::Cow, collections::BTreeSet},
};

pub use self::data::Template as Data;

pub mod data;

#[derive(Clone, Debug, Default)]
pub struct Template {
	pub components: Vec<Component>,
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
pub enum Component {
	String(String),
	Artifact(tg::Artifact),
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

	#[must_use]
	pub fn to_data(&self) -> Data {
		let components = self
			.components
			.iter()
			.map(tg::template::Component::to_data)
			.collect();
		Data { components }
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
			tg::template::data::Component::Artifact(id) => {
				Component::Artifact(tg::Artifact::with_id(id))
			},
			tg::template::data::Component::String(string) => Component::String(string),
		});
		Ok(Self::with_components(components))
	}
}

impl Data {
	#[must_use]
	pub fn with_components(
		components: impl IntoIterator<Item = tg::template::data::Component>,
	) -> Self {
		let components = components.into_iter().collect();
		Self { components }
	}

	#[must_use]
	pub fn components(&self) -> &[tg::template::data::Component] {
		&self.components
	}

	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		for component in &self.components {
			if let tg::template::data::Component::Artifact(id) = component {
				children.insert(id.clone().into());
			}
		}
	}

	pub fn try_render<'a, F>(&'a self, mut f: F) -> tg::Result<String>
	where
		F: (FnMut(&'a tg::template::data::Component) -> tg::Result<Cow<'a, str>>) + 'a,
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
		F: (FnMut(&'a tg::template::data::Component) -> Cow<'a, str>) + 'a,
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
				components.push(tg::template::data::Component::String(
					string[i..match_.start()].to_owned(),
				));
			}

			// Get and parse the ID.
			let id = captures.get(1).unwrap();
			let id = id.as_str().parse().unwrap();

			// Add an artifact component.
			components.push(tg::template::data::Component::Artifact(id));

			// Advance the cursor to the end of the match.
			i = match_.end();
		}

		// Add the remaining text as a string component.
		if i < string.len() {
			components.push(tg::template::data::Component::String(
				string[i..].to_owned(),
			));
		}

		// Create the template.
		Ok(Self { components })
	}
}

impl Component {
	#[must_use]
	pub fn to_data(&self) -> tg::template::data::Component {
		match self {
			Self::String(string) => tg::template::data::Component::String(string.clone()),
			Self::Artifact(artifact) => tg::template::data::Component::Artifact(artifact.id()),
		}
	}
}

impl TryFrom<Data> for Template {
	type Error = tg::Error;

	fn try_from(data: Data) -> Result<Self, Self::Error> {
		let components = data
			.components
			.into_iter()
			.map(TryInto::try_into)
			.collect::<tg::Result<_>>()?;
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

impl TryFrom<tg::template::data::Component> for Component {
	type Error = tg::Error;

	fn try_from(data: tg::template::data::Component) -> tg::Result<Self, Self::Error> {
		Ok(match data {
			tg::template::data::Component::String(string) => Self::String(string),
			tg::template::data::Component::Artifact(id) => {
				Self::Artifact(tg::Artifact::with_id(id))
			},
		})
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
			.id
			.clone()
			.unwrap();
		let right = id;
		assert_eq!(left, right);

		let left = template.components().get(2).unwrap().unwrap_string_ref();
		let right = " bar";
		assert_eq!(left, right);
	}
}
