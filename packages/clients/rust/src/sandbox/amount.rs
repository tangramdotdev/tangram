use crate::prelude::*;

/// CPU execution capacity in thousandths of a hardware thread.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Amount(u64);

impl Amount {
	#[must_use]
	pub const fn from_millis(millis: u64) -> Self {
		Self(millis)
	}

	#[must_use]
	pub const fn millis(self) -> u64 {
		self.0
	}

	#[must_use]
	pub const fn threads(self) -> u64 {
		self.0.div_ceil(1000)
	}
}

impl TryFrom<f64> for Amount {
	type Error = tg::Error;

	#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
	fn try_from(value: f64) -> tg::Result<Self> {
		let millis = value * 1000.0;
		if !millis.is_finite()
			|| millis < 0.0
			|| millis > f64::from(u32::MAX)
			|| (millis - millis.round()).abs() > 0.000_001
		{
			return Err(tg::error!(
				"the CPU must be a nonnegative multiple of 0.001"
			));
		}
		Ok(Self(u64::from(millis.round() as u32)))
	}
}

impl std::str::FromStr for Amount {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self> {
		let value = value
			.parse::<f64>()
			.map_err(|error| tg::error!(!error, "invalid CPU capacity"))?;
		Self::try_from(value)
	}
}

impl serde::Serialize for Amount {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		let millis = u32::try_from(self.0).map_err(serde::ser::Error::custom)?;
		if millis % 1000 == 0 {
			serializer.serialize_u32(millis / 1000)
		} else {
			serializer.serialize_f64(f64::from(millis) / 1000.0)
		}
	}
}

impl<'de> serde::Deserialize<'de> for Amount {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let value = <f64 as serde::Deserialize>::deserialize(deserializer)?;
		Self::try_from(value).map_err(serde::de::Error::custom)
	}
}

impl tangram_serialize::Serialize for Amount {
	fn serialize(&self, serializer: &mut tangram_serialize::Serializer) -> std::io::Result<()> {
		let millis = u32::try_from(self.0).map_err(std::io::Error::other)?;
		serializer.serialize(&millis)?;
		Ok(())
	}
}

impl<'de> tangram_serialize::Deserialize<'de> for Amount {
	fn deserialize(
		deserializer: &mut tangram_serialize::Deserializer<'de>,
	) -> std::io::Result<Self> {
		let millis: u32 = deserializer.deserialize()?;
		Ok(Self(u64::from(millis)))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn cpu_fractions_round_trip_without_losing_precision() {
		for value in [0.0, 0.001, 0.25, 1.0, 32.125] {
			let amount = Amount::try_from(value).unwrap();
			let json = serde_json::to_string(&amount).unwrap();
			let decoded: Amount = serde_json::from_str(&json).unwrap();
			assert_eq!(decoded, amount);
			let bytes = tangram_serialize::to_vec(&amount).unwrap();
			let decoded: Amount = tangram_serialize::from_slice(&bytes).unwrap();
			assert_eq!(decoded, amount);
		}
	}

	#[test]
	fn rejects_negative_nonfinite_and_excess_precision() {
		for value in [-1.0, f64::INFINITY, f64::NAN, 0.0001] {
			assert!(Amount::try_from(value).is_err());
		}
	}
}
