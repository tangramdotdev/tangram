use crate::prelude::*;

/// CPU limits and exclusive physical core requests.
#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct Cpu {
	#[serde(default)]
	#[tangram_serialize(default, id = 0)]
	pub dedicated: u64,
	#[serde(default)]
	#[tangram_serialize(default, id = 1)]
	pub shared: u64,
}

impl Cpu {
	pub fn validate(self) -> tg::Result<()> {
		if self.total()? == 0 {
			return Err(tg::error!("the sandbox CPU must be greater than zero"));
		}
		Ok(())
	}

	pub fn total(self) -> tg::Result<u64> {
		let total = self
			.dedicated
			.checked_add(self.shared)
			.ok_or_else(|| tg::error!("the sandbox CPU is too large"))?;
		Ok(total)
	}
}

impl From<u64> for Cpu {
	fn from(shared: u64) -> Self {
		Self {
			dedicated: 0,
			shared,
		}
	}
}
