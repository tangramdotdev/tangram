use super::Kind;
use crate as tg;
use std::path::PathBuf;

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Module {
	pub kind: Kind,
	pub referent: tg::Referent<Item>,
}

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Item {
	Path(PathBuf),
	Object(tg::object::Id),
}

impl std::fmt::Display for Module {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "({})", self.kind)?;
		if let Some(tag) = &self.referent.tag {
			write!(f, " {tag}")?;
			if let Some(subpath) = &self.referent.subpath {
				write!(f, ":{}", subpath.display())?;
			}
		} else if let Some(path) = &self.referent.path {
			write!(f, " {}", path.display())?;
			if let Some(subpath) = &self.referent.subpath {
				write!(f, "/{}", subpath.display())?;
			}
		} else {
			match &self.referent.item {
				Item::Path(path) => {
					write!(f, " {}", path.display())?;
					if let Some(subpath) = &self.referent.subpath {
						write!(f, "/{}", subpath.display())?;
					}
				},
				Item::Object(object) => {
					write!(f, " {object}")?;
					if let Some(subpath) = &self.referent.subpath {
						write!(f, ":{}", subpath.display())?;
					}
				},
			}
		}
		Ok(())
	}
}

impl std::fmt::Display for Item {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Path(path) => {
				if path
					.components()
					.next()
					.is_some_and(|component| matches!(component, std::path::Component::Normal(_)))
				{
					write!(f, "./")?;
				}
				write!(f, "{}", path.display())?;
			},
			Self::Object(object) => {
				write!(f, "{object}")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Item {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.starts_with('.') || s.starts_with('/') {
			Ok(Self::Path(s.strip_prefix("./").unwrap_or(s).into()))
		} else {
			Ok(Self::Object(s.parse()?))
		}
	}
}
