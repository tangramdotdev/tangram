use crate::prelude::*;

/// A resource reservation and an upper bound on consumption.
#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Resource<T> {
	#[tangram_serialize(id = 0)]
	pub max: T,
	#[tangram_serialize(id = 1)]
	pub min: T,
}

impl<T: Copy + Default + PartialOrd> Resource<T> {
	#[must_use]
	pub fn new(min: T, max: T) -> Self {
		Self { max, min }
	}

	pub fn validate(self) -> tg::Result<()> {
		if self.min < T::default() || self.max < self.min {
			return Err(tg::error!(
				"the resource bounds must satisfy 0 <= min <= max"
			));
		}
		Ok(())
	}
}

impl<T: Default> From<T> for Resource<T> {
	fn from(max: T) -> Self {
		Self {
			max,
			min: T::default(),
		}
	}
}

impl<'de, T> serde::Deserialize<'de> for Resource<T>
where
	T: Copy + Default + PartialOrd + serde::Deserialize<'de>,
{
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		struct Visitor<T>(std::marker::PhantomData<T>);

		impl<'de, T> serde::de::Visitor<'de> for Visitor<T>
		where
			T: Copy + Default + PartialOrd + serde::Deserialize<'de>,
		{
			type Value = Resource<T>;

			fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
				formatter.write_str("a resource amount or min/max bounds")
			}

			fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
				let max = T::deserialize(serde::de::value::U64Deserializer::<E>::new(value))?;
				let resource = Resource::from(max);
				resource.validate().map_err(E::custom)?;
				Ok(resource)
			}

			fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
				let value = u64::try_from(value).map_err(E::custom)?;
				self.visit_u64(value)
			}

			fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
				let max = T::deserialize(serde::de::value::F64Deserializer::<E>::new(value))?;
				let resource = Resource::from(max);
				resource.validate().map_err(E::custom)?;
				Ok(resource)
			}

			fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
				if let Ok(value) = value.parse::<u64>() {
					return self.visit_u64(value);
				}
				let value = value.parse::<f64>().map_err(E::custom)?;
				self.visit_f64(value)
			}

			fn visit_map<A: serde::de::MapAccess<'de>>(
				self,
				mut map: A,
			) -> Result<Self::Value, A::Error> {
				let mut max = None;
				let mut min = None;
				let mut seen = std::collections::BTreeSet::new();
				while let Some(key) = map.next_key::<String>()? {
					if !seen.insert(key.clone()) {
						return Err(serde::de::Error::custom("a resource bound is duplicated"));
					}
					match key.as_str() {
						"max" => max = map.next_value::<Option<T>>()?,
						"min" => min = map.next_value::<Option<T>>()?,
						_ => return Err(serde::de::Error::unknown_field(&key, &["min", "max"])),
					}
				}
				if max.is_none() && min.is_none() {
					return Err(serde::de::Error::custom("a resource requires min or max"));
				}
				let min = min.unwrap_or_default();
				let max = max.unwrap_or(min);
				let resource = Resource { max, min };
				resource.validate().map_err(serde::de::Error::custom)?;
				Ok(resource)
			}
		}

		deserializer.deserialize_any(Visitor(std::marker::PhantomData))
	}
}

impl Resource<u64> {
	#[must_use]
	pub fn contains(self, requested: Self, overcommit: u64) -> bool {
		let cost = u128::from(requested.min) * u128::from(overcommit)
			+ u128::from(requested.max.saturating_sub(requested.min));
		overcommit > 0 && requested.min <= self.min && cost <= u128::from(self.max)
	}

	#[must_use]
	pub fn subtract(self, requested: Self, overcommit: u64) -> Self {
		let cost = u128::from(requested.min) * u128::from(overcommit)
			+ u128::from(requested.max.saturating_sub(requested.min));
		let max = u128::from(self.max).saturating_sub(cost);
		let max = u64::try_from(max).unwrap_or(u64::MAX);
		let min = self
			.min
			.saturating_sub(requested.min)
			.min(max / overcommit.max(1));
		Self { max, min }
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn scalar_and_bounds_have_consistent_defaults() {
		for (json, expected) in [
			("4", Resource::new(0, 4)),
			(r#"{"max":4}"#, Resource::new(0, 4)),
			(r#"{"min":4}"#, Resource::new(4, 4)),
			(r#"{"min":2,"max":4}"#, Resource::new(2, 4)),
		] {
			let resource: Resource<u64> = serde_json::from_str(json).unwrap();
			assert_eq!(resource, expected);
			let bytes = tangram_serialize::to_vec(&resource).unwrap();
			let decoded: Resource<u64> = tangram_serialize::from_slice(&bytes).unwrap();
			assert_eq!(decoded, resource);
		}
	}

	#[test]
	fn query_bounds_and_json_bounds_preserve_the_same_value() {
		let arg = crate::sandbox::create::Arg {
			cpu: Some(crate::sandbox::Cpu::new(
				crate::sandbox::Amount::from_millis(250),
				crate::sandbox::Amount::from_millis(1000),
			)),
			memory: Some(Resource::new(268_435_456, 1_073_741_824)),
			storage: Some(Resource::new(1_073_741_824, 10_737_418_240)),
			swap: Some(0.into()),
			..Default::default()
		};
		let config = serde_qs::Config::new().use_form_encoding(true);
		let query = config.serialize_string(&arg).unwrap();
		let decoded: crate::sandbox::create::Arg = config.deserialize_str(&query).unwrap();
		assert_eq!(decoded.cpu, arg.cpu);
		assert_eq!(decoded.memory, arg.memory);
		assert_eq!(decoded.storage, arg.storage);
		assert_eq!(decoded.swap, arg.swap);
	}

	#[test]
	fn rejects_invalid_bounds_and_unknown_fields() {
		for json in ["{}", r#"{"min":5,"max":4}"#, r#"{"max":4,"other":1}"#] {
			assert!(serde_json::from_str::<Resource<u64>>(json).is_err());
		}
	}
}
