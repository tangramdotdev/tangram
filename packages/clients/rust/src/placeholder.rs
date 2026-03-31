use crate::prelude::*;

pub use self::data::Placeholder as Data;

pub mod data;

/// A placeholder value.
#[derive(Clone, Debug)]
pub struct Placeholder {
	pub name: String,
}

impl Placeholder {
	#[must_use]
	pub fn new(name: impl Into<String>) -> Self {
		Self { name: name.into() }
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		Data {
			name: self.name.clone(),
		}
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		Ok(Self { name: data.name })
	}
}

impl std::fmt::Display for Placeholder {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.placeholder(self)?;
		Ok(())
	}
}
