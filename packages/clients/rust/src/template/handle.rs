use {
	super::{Builder, Data},
	crate::prelude::*,
	futures::{TryStreamExt as _, stream::FuturesOrdered},
	std::borrow::Cow,
};

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
	Placeholder(tg::Placeholder),
}

impl Template {
	#[must_use]
	pub fn builder() -> Builder {
		Builder::new()
	}

	#[must_use]
	pub fn with_components(components: impl IntoIterator<Item = Component>) -> Self {
		Self::builder().components(components).build()
	}

	#[must_use]
	pub fn components(&self) -> &[Component] {
		&self.components
	}

	pub fn artifacts(&self) -> impl Iterator<Item = &tg::Artifact> {
		self.components
			.iter()
			.filter_map(|component| match component {
				Component::String(_) | Component::Placeholder(_) => None,
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

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let components = data
			.components
			.into_iter()
			.map(TryInto::try_into)
			.collect::<tg::Result<_>>()?;
		Ok(Self { components })
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
		Self::unrender_with(prefix, string, |_| Ok(None))
	}

	/// Unrender a string using a resolver for each artifact occurrence.
	///
	/// Returning `None` keeps a bare handle; a same-ID handle retains its state.
	/// Resolver errors are propagated; tokens are not verified.
	pub fn unrender_with<F>(prefix: &str, string: &str, mut f: F) -> tg::Result<Self>
	where
		F: FnMut(tg::artifact::Id) -> tg::Result<Option<tg::Artifact>>,
	{
		// Parse the template.
		let data = Data::unrender(prefix, string)?;
		let mut template = Self::try_from_data(data)?;

		// Resolve the artifacts.
		for component in &mut template.components {
			let Component::Artifact(artifact) = component else {
				continue;
			};
			let id = artifact.id();
			let Some(resolved) = f(id.clone()).map_err(
				|error| tg::error!(!error, %id, "failed to resolve an unrendered artifact"),
			)?
			else {
				continue;
			};
			if resolved.id() != id {
				return Err(
					tg::error!(expected = %id, actual = %resolved.id(), "the resolved artifact has a different ID"),
				);
			}
			*artifact = resolved;
		}

		Ok(template)
	}
}

impl Component {
	#[must_use]
	pub fn to_data(&self) -> tg::template::data::Component {
		match self {
			Self::String(string) => tg::template::data::Component::String(string.clone()),
			Self::Artifact(artifact) => {
				let artifact = artifact.to_referent();
				tg::template::data::Component::Artifact(artifact)
			},
			Self::Placeholder(placeholder) => {
				tg::template::data::Component::Placeholder(placeholder.to_data())
			},
		}
	}
}

impl From<Component> for Template {
	fn from(value: Component) -> Self {
		vec![value].into()
	}
}

impl From<Vec<Component>> for Template {
	fn from(value: Vec<Component>) -> Self {
		Self::builder().components(value).build()
	}
}

impl FromIterator<Component> for Template {
	fn from_iter<I: IntoIterator<Item = Component>>(value: I) -> Self {
		Self::builder().components(value).build()
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
			tg::template::data::Component::Artifact(referent) => {
				let artifact = tg::Artifact::with_referent(referent);
				Self::Artifact(artifact)
			},
			tg::template::data::Component::Placeholder(data) => {
				Self::Placeholder(tg::Placeholder::try_from_data(data)?)
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
	use {
		super::*,
		std::{collections::BTreeMap, error::Error as _},
	};

	// Unrendering a store path back into a template splits it into the surrounding string and artifact components.
	#[test]
	fn unrender() {
		let id = "dir_010000000000000000000000000000000000000000000000000000"
			.parse()
			.unwrap();
		let string = format!("foo /path/to/.tangram/store/{id} bar");
		let template = tg::Template::unrender("/path/to/.tangram/store", &string).unwrap();

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
			.try_get_id()
			.unwrap()
			.try_unwrap_directory()
			.unwrap();
		let right = id;
		assert_eq!(left, right);

		let left = template.components().get(2).unwrap().unwrap_string_ref();
		let right = " bar";
		assert_eq!(left, right);
	}

	#[test]
	fn unrender_with() {
		let directory = tg::Directory::with_entries(BTreeMap::new());
		directory
			.state()
			.set_location(Some("remote".parse().unwrap()));
		let id: tg::artifact::Id = directory.id().into();
		let file = tg::File::with_contents("unresolved").id();
		let string = format!("α -L/store/{id}/lib /store/{file} /store/{id}/include");
		let mut ids = Vec::new();
		let template = tg::Template::unrender_with("/store", &string, |parsed| {
			ids.push(parsed.clone());
			Ok((parsed == id).then(|| directory.clone().into()))
		})
		.unwrap();
		assert_eq!(ids, [id.clone(), file.clone().into(), id]);
		let expected = tg::Template::with_components([
			Component::String("α -L".into()),
			Component::Artifact(directory.clone().into()),
			Component::String("/lib ".into()),
			Component::Artifact(tg::File::with_id(file).into()),
			Component::String(" ".into()),
			Component::Artifact(directory.clone().into()),
			Component::String("/include".into()),
		]);
		assert_eq!(template.to_data(), expected.to_data());
		for artifact in template
			.artifacts()
			.filter(|artifact| artifact.is_directory())
		{
			assert_eq!(artifact.state().identity(), directory.state().identity());
		}
		let unresolved = template.artifacts().nth(1).unwrap();
		assert!(unresolved.state().object().is_none());
	}

	#[test]
	fn unrender_with_different_id() {
		let file = tg::File::with_contents("original");
		let other = tg::File::with_contents("other");
		let string = format!("/store/{}", file.id());
		let error =
			tg::Template::unrender_with("/store", &string, |_| Ok(Some(other.clone().into())))
				.unwrap_err();
		assert_eq!(
			error.message().unwrap(),
			"the resolved artifact has a different ID"
		);
	}

	#[test]
	fn unrender_with_error() {
		let id = tg::File::with_contents("file").id();
		let string = format!("/store/{id} /store/{id}");
		let mut calls = 0;
		let error = tg::Template::unrender_with("/store", &string, |_| {
			calls += 1;
			Err(tg::error!("invalid checkout metadata"))
		})
		.unwrap_err();
		assert_eq!(calls, 1);
		assert_eq!(
			error.source().unwrap().to_string(),
			"invalid checkout metadata"
		);
	}

	#[test]
	fn unrender_with_text() {
		for string in ["", "ordinary text", "/store/fil_invalid"] {
			let template = tg::Template::unrender_with("/store", string, |_| {
				panic!("text should not invoke the resolver")
			})
			.unwrap();
			let rendered = template
				.try_render_sync(|component| Ok(Cow::Borrowed(component.unwrap_string_ref())))
				.unwrap();
			assert_eq!(rendered, string);
		}
	}
}
