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
		todo!()
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

fn pattern(input: &mut &str) -> PResult<Pattern> {
	let components = separated(1.., component, ",").parse_next(input)?;
	Ok(Pattern { components })
}

fn component(input: &mut &str) -> PResult<Component> {
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

fn operator(input: &mut &str) -> PResult<Operator> {
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
