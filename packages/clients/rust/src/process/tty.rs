use crate::prelude::*;

pub mod size;

#[derive(
	Copy,
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Tty {
	#[tangram_serialize(id = 0)]
	pub size: tg::process::tty::Size,
}

#[derive(
	Copy,
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Size {
	#[tangram_serialize(id = 0)]
	pub rows: u16,

	#[tangram_serialize(id = 1)]
	pub cols: u16,
}

impl std::fmt::Display for Size {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{},{}", self.rows, self.cols)
	}
}

impl std::str::FromStr for Size {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let (rows, cols) = s
			.split_once(',')
			.ok_or_else(|| tg::error!(%s, "invalid tty size"))?;
		let rows = rows
			.parse()
			.map_err(|source| tg::error!(!source, %s, "invalid tty size"))?;
		let cols = cols
			.parse()
			.map_err(|source| tg::error!(!source, %s, "invalid tty size"))?;
		Ok(Self { rows, cols })
	}
}
