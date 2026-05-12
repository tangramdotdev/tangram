use crate::prelude::*;

pub mod create;
pub mod delete;
pub mod get;
pub mod grants;

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Namespace(Vec<String>);

impl Namespace {
	#[must_use]
	pub fn root() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn with_components(components: impl IntoIterator<Item = String>) -> Self {
		Self(components.into_iter().collect())
	}

	#[must_use]
	pub fn is_root(&self) -> bool {
		self.0.is_empty()
	}

	pub fn components(&self) -> impl Iterator<Item = &str> {
		self.0.iter().map(String::as_str)
	}
}

impl std::fmt::Display for Namespace {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0.join("/"))
	}
}

impl std::str::FromStr for Namespace {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.is_empty() {
			return Ok(Self::root());
		}
		let mut components = Vec::new();
		for component in s.split('/') {
			if component.is_empty() || tg::list::pattern::contains_operators(component) {
				return Err(tg::error!("invalid namespace"));
			}
			component
				.parse::<tg::tag::Name>()
				.map_err(|_| tg::error!("invalid namespace"))?;
			components.push(component.to_owned());
		}
		Ok(Self(components))
	}
}
