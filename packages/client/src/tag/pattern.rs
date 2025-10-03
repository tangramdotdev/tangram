use {crate as tg, std::cmp::Ordering};

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
)]
pub struct Pattern(String);

impl Pattern {
	#[must_use]
	pub fn new(s: impl Into<String>) -> Self {
		Self(s.into())
	}

	#[must_use]
	pub fn as_str(&self) -> &str {
		&self.0
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn components(&self) -> impl Iterator<Item = &str> {
		self.0.split('/')
	}

	#[must_use]
	pub fn matches(&self, tag: &tg::Tag) -> bool {
		let mut t = tag.components();
		let mut p = self.components();
		for (t, p) in t.by_ref().zip(p.by_ref()) {
			if !matches(t, p) {
				return false;
			}
		}
		true
	}

	pub fn push(&mut self, component: &str) {
		if !self.is_empty() {
			self.0.push('/');
		}
		self.0.push_str(component);
	}

	#[must_use]
	pub fn parent(&self) -> Option<Self> {
		let components: Vec<_> = self.0.split('/').collect();
		if components.len() <= 1 {
			return None;
		}
		let parent = components[..components.len() - 1].join("/");
		Some(Self(parent))
	}
}

impl AsRef<str> for Pattern {
	fn as_ref(&self) -> &str {
		self.0.as_str()
	}
}

impl std::fmt::Display for Pattern {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl std::str::FromStr for Pattern {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Ok(Self(s.to_owned()))
	}
}

impl TryFrom<Pattern> for tg::Tag {
	type Error = tg::Error;

	fn try_from(value: tg::tag::Pattern) -> Result<Self, Self::Error> {
		if value.0.contains(['=', '>', '<', '^']) {
			return Err(tg::error!("the pattern contains operators"));
		}
		Ok(Self(value.0))
	}
}

impl From<tg::Tag> for Pattern {
	fn from(value: tg::Tag) -> Self {
		Self(value.as_str().to_owned())
	}
}

#[must_use]
pub fn matches(t: &str, p: &str) -> bool {
	for p in p.split(',') {
		if p == "*" {
		} else if let Some(p) = p.strip_prefix("^") {
			let order = compare(t, p);
			if !matches!(order, Ordering::Greater | Ordering::Equal) {
				return false;
			}
			let next = next(p);
			let order = compare(t, &next);
			if !matches!(order, Ordering::Less) {
				return false;
			}
		} else if let Some(p) = p.strip_prefix(">=") {
			let order = compare(t, p);
			if !matches!(order, Ordering::Greater | Ordering::Equal) {
				return false;
			}
		} else if let Some(p) = p.strip_prefix('>') {
			let order = compare(t, p);
			if !matches!(order, Ordering::Greater) {
				return false;
			}
		} else if let Some(p) = p.strip_prefix("<=") {
			let order = compare(t, p);
			if !matches!(order, Ordering::Less | Ordering::Equal) {
				return false;
			}
		} else if let Some(p) = p.strip_prefix('<') {
			let order = compare(t, p);
			if !matches!(order, Ordering::Less) {
				return false;
			}
		} else if let Some(p) = p.strip_prefix('=') {
			let order = compare(t, p);
			if !matches!(order, Ordering::Equal) {
				return false;
			}
		} else if t != p {
			return false;
		}
	}
	true
}

fn next(p: &str) -> String {
	let mut chars = p.chars().peekable();
	let mut output = String::new();
	while chars.peek().is_some_and(|c| !c.is_alphanumeric()) {
		output.push(chars.next().unwrap());
	}
	if chars.peek().is_some_and(char::is_ascii_digit) {
		let mut s = String::new();
		while chars.peek().is_some_and(char::is_ascii_digit) {
			s.push(chars.next().unwrap());
		}
		let n = s.parse::<u64>().unwrap();
		let s = (n + 1).to_string();
		output.push_str(&s);
	} else {
		while chars.peek().is_some_and(char::is_ascii_alphabetic) {
			output.push(chars.next().unwrap());
		}
		output.push('0');
	}
	output.extend(chars);
	output
}

pub fn compare(a: &str, b: &str) -> Ordering {
	let mut a_chars = a.chars().peekable();
	let mut b_chars = b.chars().peekable();
	loop {
		while a_chars
			.peek()
			.is_some_and(|c| !c.is_alphanumeric() && *c != '~')
		{
			a_chars.next();
		}
		while b_chars
			.peek()
			.is_some_and(|c| !c.is_alphanumeric() && *c != '~')
		{
			b_chars.next();
		}
		let a_is_tilde = a_chars.peek() == Some(&'~');
		let b_is_tilde = b_chars.peek() == Some(&'~');
		if a_is_tilde && b_is_tilde {
			a_chars.next();
			b_chars.next();
			continue;
		}
		if a_is_tilde {
			return Ordering::Less;
		}
		if b_is_tilde {
			return Ordering::Greater;
		}
		if a_chars.peek().is_none() && b_chars.peek().is_none() {
			return Ordering::Equal;
		}
		if a_chars.peek().is_none() {
			return Ordering::Less;
		}
		if b_chars.peek().is_none() {
			return Ordering::Greater;
		}
		let a_is_digit = a_chars.peek().unwrap().is_ascii_digit();
		let b_is_digit = b_chars.peek().unwrap().is_ascii_digit();
		match (a_is_digit, b_is_digit) {
			(true, false) => return Ordering::Greater,
			(false, true) => return Ordering::Less,
			_ => {},
		}
		if a_is_digit {
			let mut a_num_str = String::new();
			while a_chars.peek().is_some_and(char::is_ascii_digit) {
				a_num_str.push(a_chars.next().unwrap());
			}
			let mut b_num_str = String::new();
			while b_chars.peek().is_some_and(char::is_ascii_digit) {
				b_num_str.push(b_chars.next().unwrap());
			}
			let a_num: u64 = a_num_str.parse().unwrap_or(0);
			let b_num: u64 = b_num_str.parse().unwrap_or(0);
			match a_num.cmp(&b_num) {
				Ordering::Equal => {},
				other => return other,
			}
		} else {
			let mut a_alpha = String::new();
			while a_chars.peek().is_some_and(char::is_ascii_alphabetic) {
				a_alpha.push(a_chars.next().unwrap());
			}
			let mut b_alpha = String::new();
			while b_chars.peek().is_some_and(char::is_ascii_alphabetic) {
				b_alpha.push(b_chars.next().unwrap());
			}
			match a_alpha.cmp(&b_alpha) {
				Ordering::Equal => {},
				other => return other,
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_compare_numeric_leading_zeros() {
		assert_eq!(compare("1.0010", "1.10"), Ordering::Equal);
		assert_eq!(compare("1.05", "1.5"), Ordering::Equal);
		assert_eq!(compare("010", "10"), Ordering::Equal);
		assert_eq!(compare("000", "0"), Ordering::Equal);
	}

	#[test]
	fn test_compare_numeric_segments() {
		assert_eq!(compare("1.0010", "1.9"), Ordering::Greater);
		assert_eq!(compare("2.50", "2.5"), Ordering::Greater);
		assert_eq!(compare("10", "2"), Ordering::Greater);
		assert_eq!(compare("1.2", "1.10"), Ordering::Less);
	}

	#[test]
	fn test_compare_length_differs() {
		assert_eq!(compare("1.0", "1"), Ordering::Greater);
		assert_eq!(compare("1", "1.0"), Ordering::Less);
		assert_eq!(compare("1.2.3", "1.2"), Ordering::Greater);
		assert_eq!(compare("1.2", "1.2.3"), Ordering::Less);
	}

	#[test]
	fn test_compare_separators_ignored() {
		assert_eq!(compare("fc4", "fc.4"), Ordering::Equal);
		assert_eq!(compare("2xFg33.+f.5", "2xFg33f5"), Ordering::Equal);
		assert_eq!(compare("1-2-3", "1.2.3"), Ordering::Equal);
		assert_eq!(compare("a_b_c", "a.b.c"), Ordering::Equal);
	}

	#[test]
	fn test_compare_numeric_vs_alphabetic() {
		assert_eq!(compare("2a", "2.0"), Ordering::Less);
		assert_eq!(compare("2.0", "2a"), Ordering::Greater);
		assert_eq!(compare("1.0", "1.fc4"), Ordering::Greater);
		assert_eq!(compare("1.fc4", "1.0"), Ordering::Less);
		assert_eq!(compare("0", "Z"), Ordering::Greater);
	}

	#[test]
	fn test_compare_alphabetic_case_sensitive() {
		assert_eq!(compare("FC5", "fc4"), Ordering::Less);
		assert_eq!(compare("fc4", "FC5"), Ordering::Greater);
		assert_eq!(compare("add", "ZULU"), Ordering::Greater);
		assert_eq!(compare("ZULU", "add"), Ordering::Less);
	}

	#[test]
	fn test_compare_alphabetic_lexicographic() {
		assert_eq!(compare("b", "a"), Ordering::Greater);
		assert_eq!(compare("a", "b"), Ordering::Less);
		assert_eq!(compare("abc", "abd"), Ordering::Less);
		assert_eq!(compare("xyz", "abc"), Ordering::Greater);
	}

	#[test]
	fn test_compare_equal() {
		assert_eq!(compare("1.0", "1.0"), Ordering::Equal);
		assert_eq!(compare("abc", "abc"), Ordering::Equal);
		assert_eq!(compare("1.2.3", "1.2.3"), Ordering::Equal);
		assert_eq!(compare("", ""), Ordering::Equal);
	}

	#[test]
	fn test_compare_empty_strings() {
		assert_eq!(compare("", "1"), Ordering::Less);
		assert_eq!(compare("1", ""), Ordering::Greater);
		assert_eq!(compare("", "a"), Ordering::Less);
		assert_eq!(compare("a", ""), Ordering::Greater);
	}

	#[test]
	fn test_compare_complex_mixed() {
		assert_eq!(compare("1.2alpha3", "1.2alpha4"), Ordering::Less);
		assert_eq!(compare("1.2alpha4", "1.2alpha3"), Ordering::Greater);
		assert_eq!(compare("1.2.3alpha", "1.2.3beta"), Ordering::Less);
		assert_eq!(compare("1.2.3~rc1", "1.2.3"), Ordering::Less);
	}

	#[test]
	fn test_compare_tilde() {
		assert_eq!(compare("1.0~rc1", "1.0"), Ordering::Less);
		assert_eq!(compare("1.0", "1.0~rc1"), Ordering::Greater);
		assert_eq!(compare("1.0~alpha", "1.0~beta"), Ordering::Less);
		assert_eq!(compare("1.0~~", "1.0~"), Ordering::Less);
		assert_eq!(compare("1.0~", "1.0"), Ordering::Less);
	}

	#[test]
	fn test_compare_all_separators() {
		assert_eq!(compare("1...2", "1.2"), Ordering::Equal);
		assert_eq!(compare("1---2", "1.2"), Ordering::Equal);
		assert_eq!(compare("1@#$%2", "1.2"), Ordering::Equal);
	}

	#[test]
	fn test_compare_realistic_versions() {
		assert_eq!(compare("1.0.0", "0.9.9"), Ordering::Greater);
		assert_eq!(compare("2.1.0", "2.0.99"), Ordering::Greater);
		assert_eq!(compare("1.0.0-alpha", "1.0.0-beta"), Ordering::Less);
		assert_eq!(compare("1.0.0-rc1", "1.0.0-rc2"), Ordering::Less);
		assert_eq!(compare("3.14.159", "3.14.16"), Ordering::Greater);
	}

	#[test]
	fn test_matches_star() {
		assert!(matches("1.0.0", "*"));
		assert!(matches("anything", "*"));
		assert!(matches("", "*"));
	}

	#[test]
	fn test_matches_exact() {
		assert!(matches("1.0.0", "1.0.0"));
		assert!(matches("1.2.3", "1.2.3"));
		assert!(!matches("1.0.0", "1.0.1"));
		assert!(!matches("1.2.3", "1.2.4"));
	}

	#[test]
	fn test_matches_greater_than() {
		assert!(matches("2.0.0", ">1.0.0"));
		assert!(matches("1.5", ">1.0"));
		assert!(!matches("1.0.0", ">1.0.0"));
		assert!(!matches("0.9", ">1.0"));
	}

	#[test]
	fn test_matches_greater_than_or_equal() {
		assert!(matches("2.0.0", ">=1.0.0"));
		assert!(matches("1.0.0", ">=1.0.0"));
		assert!(matches("1.5", ">=1.0"));
		assert!(!matches("0.9", ">=1.0"));
	}

	#[test]
	fn test_matches_less_than() {
		assert!(matches("0.9", "<1.0"));
		assert!(matches("1.0~rc1", "<1.0"));
		assert!(!matches("1.0", "<1.0"));
		assert!(!matches("2.0", "<1.0"));
	}

	#[test]
	fn test_matches_less_than_or_equal() {
		assert!(matches("0.9", "<=1.0"));
		assert!(matches("1.0", "<=1.0"));
		assert!(matches("1.0~rc1", "<=1.0"));
		assert!(!matches("1.1", "<=1.0"));
	}

	#[test]
	fn test_matches_equal_operator() {
		assert!(matches("1.0.0", "=1.0.0"));
		assert!(matches("1.2.3", "=1.2.3"));
		assert!(!matches("1.0.1", "=1.0.0"));
	}

	#[test]
	fn test_matches_caret() {
		assert!(matches("1.2.3", "^1.2.3"));
		assert!(matches("1.2.4", "^1.2.3"));
		assert!(matches("1.99.99", "^1.2.3"));
		assert!(matches("2.0.0", "^1.2.3"));
		assert!(matches("2.2.2", "^1.2.3"));
		assert!(!matches("2.2.3", "^1.2.3"));
		assert!(!matches("2.3.0", "^1.2.3"));
		assert!(!matches("1.2.2", "^1.2.3"));
		assert!(!matches("3.0.0", "^1.2.3"));
	}

	#[test]
	fn test_matches_multiple_constraints() {
		assert!(matches("1.5", ">=1.0,<2.0"));
		assert!(matches("1.0", ">=1.0,<2.0"));
		assert!(matches("1.99", ">=1.0,<2.0"));
		assert!(!matches("0.9", ">=1.0,<2.0"));
		assert!(!matches("2.0", ">=1.0,<2.0"));
		assert!(!matches("2.1", ">=1.0,<2.0"));
	}

	#[test]
	fn test_matches_with_tilde() {
		assert!(matches("1.0~rc1", "<1.0"));
		assert!(matches("1.0~rc2", ">1.0~rc1"));
		assert!(matches("1.0~beta", ">=1.0~alpha"));
		assert!(!matches("1.0", "<1.0~rc1"));
	}

	#[test]
	fn test_matches_complex_patterns() {
		assert!(matches("1.2.5", ">=1.2.0,<1.3.0"));
		assert!(matches("2.0.0", ">1.0.0,<3.0.0"));
		assert!(matches("1.5.10", ">=1.5.0,<=1.5.10"));
		assert!(!matches("1.5.11", ">=1.5.0,<=1.5.10"));
	}
}
