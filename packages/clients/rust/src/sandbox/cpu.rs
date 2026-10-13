use crate::prelude::*;

pub type Cpu = super::Resource<super::Amount>;

impl Cpu {
	pub fn total(self) -> tg::Result<u64> {
		self.validate()?;
		if self.max.millis() == 0 || self.max.millis() > u64::from(u32::MAX) {
			return Err(tg::error!(
				"the sandbox CPU maximum must be between 0.001 and 4294967.295"
			));
		}
		Ok(self.max.threads())
	}
}

impl From<u64> for Cpu {
	fn from(threads: u64) -> Self {
		Self::from(super::Amount::from_millis(threads.saturating_mul(1000)))
	}
}
