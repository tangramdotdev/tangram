use crate::prelude::*;

pub type Cpu = super::Resource<super::Amount>;

impl Cpu {
	/// Get the number of hardware threads that the maximum spans, where none means no limit.
	pub fn threads(self) -> tg::Result<Option<u64>> {
		self.validate()?;
		let Some(max) = self.max else {
			return Ok(None);
		};
		if max.millis() == 0 || max.millis() > u64::from(u32::MAX) {
			return Err(tg::error!(
				"the sandbox CPU maximum must be between 0.001 and 4294967.295"
			));
		}
		Ok(Some(max.threads()))
	}
}

impl From<u64> for Cpu {
	fn from(threads: u64) -> Self {
		Self::from(super::Amount::from_millis(threads.saturating_mul(1000)))
	}
}
