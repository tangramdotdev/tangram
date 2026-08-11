use crate::prelude::*;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	derive_more::IsVariant,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Selector<I> {
	#[tangram_serialize(id = 0)]
	#[display("{_0}")]
	Id(I),

	#[tangram_serialize(id = 1)]
	#[display("{_0}")]
	Specifier(tg::Specifier),
}

impl<I> From<tg::Specifier> for Selector<I> {
	fn from(value: tg::Specifier) -> Self {
		Self::Specifier(value)
	}
}

impl<I> std::str::FromStr for Selector<I>
where
	I: std::str::FromStr<Err = tg::Error>,
{
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(id) = s.parse() {
			Ok(Self::Id(id))
		} else {
			Ok(Self::Specifier(s.parse()?))
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// A selector retains its semantic variant in the Tangram representation.
	#[test]
	fn tangram_tagged() {
		let selector = Selector::<u64>::Id(42);
		let bytes = tangram_serialize::to_vec(&selector).unwrap();
		let value = tangram_serialize::from_slice::<tangram_serialize::Value>(&bytes).unwrap();
		let tangram_serialize::Value::Enum(value) = value else {
			panic!("expected an enum");
		};
		assert_eq!(value.id, 0);
		let actual = tangram_serialize::from_slice::<Selector<u64>>(&bytes).unwrap();
		assert_eq!(actual, selector);
	}
}
