use crate::Version;
use winnow::{
	ascii::dec_uint,
	combinator::{alt, opt, preceded, separated},
	prelude::*,
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Pattern {
	pub components: Vec<Component>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Component {
	pub operator: Operator,
	pub major: u64,
	pub minor: Option<u64>,
	pub patch: Option<u64>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum Operator {
	Caret,
	Eq,
	Greater,
	GreaterEq,
	Less,
	LessEq,
	Tilde,
}

#[derive(Clone, Debug, derive_more::Display, derive_more::Error)]
#[display("parse error")]
pub struct ParseError;

impl Pattern {
	#[must_use]
	pub fn matches(&self, version: &Version) -> bool {
		self.components
			.iter()
			.all(|component| component.matches(version))
	}
}

impl Component {
	#[must_use]
	pub fn matches(&self, version: &Version) -> bool {
		match self.operator {
			Operator::Caret => self.matches_caret(version),
			Operator::Eq => self.matches_eq(version),
			Operator::Greater => self.matches_greater(version),
			Operator::GreaterEq => self.matches_greater(version) || self.matches_eq(version),
			Operator::Less => self.matches_less(version),
			Operator::LessEq => self.matches_less(version) || self.matches_eq(version),
			Operator::Tilde => self.matches_tilde(version),
		}
	}

	fn matches_caret(&self, version: &Version) -> bool {
		if version.major != self.major {
			return false;
		}
		let minor = match self.minor {
			None => return true,
			Some(minor) => minor,
		};
		let patch = match self.patch {
			None => {
				if self.major > 0 {
					return version.minor >= minor;
				}
				return version.minor == minor;
			},
			Some(patch) => patch,
		};
		if self.major > 0 {
			if version.minor != minor {
				return version.minor > minor;
			} else if version.patch != patch {
				return version.patch > patch;
			}
		} else if minor > 0 {
			if version.minor != minor {
				return false;
			} else if version.patch != patch {
				return version.patch > patch;
			}
		} else if version.minor != minor || version.patch != patch {
			return false;
		}
		true
	}

	fn matches_eq(&self, version: &Version) -> bool {
		if version.major != self.major {
			return false;
		}
		if let Some(minor) = self.minor {
			if version.minor != minor {
				return false;
			}
		}
		if let Some(patch) = self.patch {
			if version.patch != patch {
				return false;
			}
		}
		true
	}

	fn matches_greater(&self, version: &Version) -> bool {
		if version.major != self.major {
			return version.major > self.major;
		}
		match self.minor {
			None => {
				return false;
			},
			Some(minor) => {
				if version.minor != minor {
					return version.minor > minor;
				}
			},
		}
		match self.patch {
			None => {
				return false;
			},
			Some(patch) => {
				if version.patch != patch {
					return version.patch > patch;
				}
			},
		}
		true
	}

	fn matches_less(&self, version: &Version) -> bool {
		if version.major != self.major {
			return version.major < self.major;
		}
		match self.minor {
			None => {
				return false;
			},
			Some(minor) => {
				if version.minor != minor {
					return version.minor < minor;
				}
			},
		}
		match self.patch {
			None => {
				return false;
			},
			Some(patch) => {
				if version.patch != patch {
					return version.patch < patch;
				}
			},
		}
		true
	}

	fn matches_tilde(&self, version: &Version) -> bool {
		if version.major != self.major {
			return false;
		}
		if let Some(minor) = self.minor {
			if version.minor != minor {
				return false;
			}
		}
		if let Some(patch) = self.patch {
			if version.patch != patch {
				return version.patch > patch;
			}
		}
		true
	}
}

impl std::fmt::Display for Pattern {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for (i, component) in self.components.iter().enumerate() {
			write!(f, "{component}")?;
			if i != self.components.len() - 1 {
				write!(f, ",")?;
			}
		}
		Ok(())
	}
}

impl std::str::FromStr for Pattern {
	type Err = ParseError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		pattern.parse(s).ok().ok_or(ParseError)
	}
}

impl std::fmt::Display for Component {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.operator)?;
		write!(f, "{}", self.major)?;
		if let Some(minor) = self.minor {
			write!(f, ".{minor}")?;
		}
		if let Some(patch) = self.patch {
			write!(f, ".{patch}")?;
		}
		Ok(())
	}
}

impl std::str::FromStr for Component {
	type Err = ParseError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		component.parse(s).ok().ok_or(ParseError)
	}
}

impl std::fmt::Display for Operator {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Operator::Caret => write!(f, "^"),
			Operator::Eq => write!(f, "="),
			Operator::Greater => write!(f, ">"),
			Operator::GreaterEq => write!(f, ">="),
			Operator::Less => write!(f, "<"),
			Operator::LessEq => write!(f, "<="),
			Operator::Tilde => write!(f, "~"),
		}
	}
}

impl std::str::FromStr for Operator {
	type Err = ParseError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		operator.parse(s).ok().ok_or(ParseError)
	}
}

fn pattern(input: &mut &str) -> ModalResult<Pattern> {
	let components = separated(1.., component, ",").parse_next(input)?;
	Ok(Pattern { components })
}

fn component(input: &mut &str) -> ModalResult<Component> {
	let (operator, major, minor, patch) = (
		operator,
		dec_uint,
		opt(preceded(".", dec_uint)),
		opt(preceded(".", dec_uint)),
	)
		.parse_next(input)?;
	Ok(Component {
		operator,
		major,
		minor,
		patch,
	})
}

fn operator(input: &mut &str) -> ModalResult<Operator> {
	alt((
		"^".map(|_| Operator::Caret),
		"=".map(|_| Operator::Eq),
		">=".map(|_| Operator::GreaterEq),
		">".map(|_| Operator::Greater),
		"<=".map(|_| Operator::LessEq),
		"<".map(|_| Operator::Less),
		"~".map(|_| Operator::Tilde),
	))
	.parse_next(input)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse() {
		let left = "=1.2.3";
		let right = Pattern {
			components: vec![Component {
				operator: Operator::Eq,
				major: 1,
				minor: Some(2),
				patch: Some(3),
			}],
		};
		assert_eq!(left.parse::<Pattern>().unwrap(), right);

		let left = ">=1.2.3,<1.5";
		let right = Pattern {
			components: vec![
				Component {
					operator: Operator::GreaterEq,
					major: 1,
					minor: Some(2),
					patch: Some(3),
				},
				Component {
					operator: Operator::Less,
					major: 1,
					minor: Some(5),
					patch: None,
				},
			],
		};
		assert_eq!(left.parse::<Pattern>().unwrap(), right);
	}
}
