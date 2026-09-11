#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Capacity {
	pub available: u64,
	pub total: u64,
}

impl Capacity {
	#[must_use]
	#[expect(
		clippy::cast_precision_loss,
		reason = "the ratio does not require integer precision"
	)]
	pub fn available_ratio(self) -> f64 {
		if self.total == 0 {
			return 0.0;
		}

		self.available as f64 / self.total as f64
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn computes_the_available_ratio() {
		let capacity = Capacity {
			available: 25,
			total: 100,
		};
		assert!((capacity.available_ratio() - 0.25).abs() < f64::EPSILON);
		let capacity = Capacity {
			available: 0,
			total: 0,
		};
		assert!(capacity.available_ratio().abs() < f64::EPSILON);
	}
}
