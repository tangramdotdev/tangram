pub use self::component::Component;
use crate::{object, Artifact, Error, Handle, Result};
use futures::{stream::FuturesOrdered, Future, TryStreamExt};
use itertools::Itertools;
use std::borrow::Cow;

#[derive(Clone, Debug, Default)]
pub struct Template {
	pub components: Vec<Component>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub components: Vec<component::Data>,
}

impl Template {
	#[must_use]
	pub fn components(&self) -> &[Component] {
		&self.components
	}

	pub fn artifacts(&self) -> impl Iterator<Item = &Artifact> {
		self.components
			.iter()
			.filter_map(|component| match component {
				Component::String(_) => None,
				Component::Artifact(artifact) => Some(artifact),
			})
	}

	#[must_use]
	pub fn objects(&self) -> Vec<object::Handle> {
		self.artifacts()
			.map(|artifact| artifact.clone().into())
			.collect()
	}

	pub fn try_render_sync<'a, F>(&'a self, mut f: F) -> Result<String>
	where
		F: (FnMut(&'a Component) -> Result<Cow<'a, str>>) + 'a,
	{
		let mut string = String::new();
		for component in &self.components {
			string.push_str(&f(component)?);
		}
		Ok(string)
	}

	pub async fn try_render<'a, F, Fut>(&'a self, f: F) -> Result<String>
	where
		F: (FnMut(&'a Component) -> Fut) + 'a,
		Fut: Future<Output = Result<String>> + 'a,
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

	pub fn unrender(string: &str) -> Result<Self> {
		// Create the regex.
		let regex =
			r"/\.tangram/artifacts/((?:fil_|dir_|sym_)01[0123456789abcdefghjkmnpqrstvwxyz]{52})";
		let regex = regex::Regex::new(regex).unwrap();

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
			let id = id.as_str().parse().unwrap();

			// Add an artifact component.
			components.push(Component::Artifact(Artifact::with_id(id)));

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

	pub async fn data(&self, tg: &impl Handle) -> Result<Data> {
		let components = self
			.components
			.iter()
			.map(|component| component.data(tg))
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		Ok(Data { components })
	}
}

impl TryFrom<Data> for Template {
	type Error = Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let components = data
			.components
			.into_iter()
			.map(TryInto::try_into)
			.try_collect()?;
		Ok(Self { components })
	}
}

impl std::fmt::Display for Template {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "`")?;
		for component in &self.components {
			match component {
				Component::String(string) => {
					write!(f, "{string}")?;
				},
				Component::Artifact(artifact) => {
					write!(f, "${{{artifact}}}")?;
				},
			}
		}
		write!(f, "`")
	}
}

impl Data {
	#[must_use]
	pub fn children(&self) -> Vec<object::Id> {
		self.components
			.iter()
			.filter_map(|component| match component {
				component::Data::String(_) => None,
				component::Data::Artifact(id) => Some(id.clone().into()),
			})
			.collect()
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
	use crate::{artifact, Artifact, Error, Handle, Result};
	use derive_more::{From, TryUnwrap};

	#[derive(Clone, Debug, From, TryUnwrap)]
	#[try_unwrap(ref)]
	pub enum Component {
		String(String),
		Artifact(Artifact),
	}

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
	pub enum Data {
		String(String),
		Artifact(artifact::Id),
	}

	impl Component {
		pub async fn data(&self, tg: &impl Handle) -> Result<Data> {
			match self {
				Self::String(string) => Ok(Data::String(string.clone())),
				Self::Artifact(artifact) => Ok(Data::Artifact(artifact.id(tg).await?)),
			}
		}
	}

	impl TryFrom<Data> for Component {
		type Error = Error;

		fn try_from(data: Data) -> Result<Self, Self::Error> {
			Ok(match data {
				Data::String(string) => Self::String(string),
				Data::Artifact(id) => Self::Artifact(Artifact::with_id(id)),
			})
		}
	}
}
