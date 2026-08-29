mod delete;
mod get;
mod key;
mod put;

pub(super) use key::Key;

#[derive(
	Clone, Copy, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(crate) enum GrantSource {
	#[tangram_serialize(id = 0)]
	Explicit,
	#[tangram_serialize(id = 1)]
	Implicit,
	#[tangram_serialize(id = 2)]
	Materialized,
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[allow(clippy::option_option)]
pub(crate) struct GrantValue {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "tangram_util::serde::is_false")]
	pub explicit: bool,

	#[tangram_serialize(
		default,
		id = 1,
		skip_serializing_if = "Option::is_none",
		with = "tangram_serialize::with::unwrap_or_skip"
	)]
	pub implicit: Option<Option<i64>>,

	#[tangram_serialize(
		default,
		id = 2,
		skip_serializing_if = "Option::is_none",
		with = "tangram_serialize::with::unwrap_or_skip"
	)]
	pub materialized: Option<Option<i64>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::option_option)]
pub(crate) struct GrantEntry {
	pub creator: Option<tangram_client::Principal>,
	pub explicit: bool,
	pub implicit: Option<Option<i64>>,
	pub materialized: Option<Option<i64>>,
	pub permission: tangram_client::authorization::Permission,
	pub subject: tangram_client::authorization::Subject,
}

#[derive(Clone)]
pub(crate) struct GrantIndexEntry<'a> {
	pub creator: Option<&'a tangram_client::Principal>,
	pub expires_at: Option<i64>,
	pub permission: tangram_client::authorization::Permission,
	pub subject: &'a tangram_client::authorization::Subject,
	pub resource: &'a tangram_client::Id,
}

impl GrantValue {
	pub(crate) fn deserialize(bytes: &[u8]) -> tangram_client::Result<Self> {
		tangram_serialize::from_slice(bytes).map_err(|error| {
			tangram_client::error!(!error, "failed to deserialize the grant value")
		})
	}

	pub(crate) fn is_empty(&self) -> bool {
		!self.explicit && self.implicit.is_none() && self.materialized.is_none()
	}

	pub(crate) fn serialize(&self) -> tangram_client::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tangram_client::error!(!error, "failed to serialize the grant value"))
	}

	#[allow(clippy::option_option)]
	pub(crate) fn source_expires_at(&self, source: GrantSource) -> Option<Option<i64>> {
		match source {
			GrantSource::Explicit => self.explicit.then_some(None),
			GrantSource::Implicit => self.implicit,
			GrantSource::Materialized => self.materialized,
		}
	}

	pub(crate) fn put(
		&mut self,
		source: GrantSource,
		expires_at: Option<i64>,
		time_to_touch: Option<std::time::Duration>,
	) -> bool {
		match source {
			GrantSource::Explicit => {
				if self.explicit {
					false
				} else {
					self.explicit = true;
					true
				}
			},
			GrantSource::Implicit => {
				if self.implicit == Some(None) {
					return false;
				}
				let time_to_touch = time_to_touch
					.map(|value| i64::try_from(value.as_secs()).unwrap())
					.unwrap_or_default();
				if let (Some(Some(current)), Some(expires_at)) = (self.implicit, expires_at)
					&& (current >= expires_at || expires_at.saturating_sub(current) < time_to_touch)
				{
					return false;
				}
				self.implicit = Some(expires_at);
				true
			},
			GrantSource::Materialized => {
				if self.materialized == Some(expires_at) {
					false
				} else {
					self.materialized = Some(expires_at);
					true
				}
			},
		}
	}

	pub(crate) fn delete(&mut self, source: GrantSource, expires_at: Option<i64>) -> bool {
		match source {
			GrantSource::Explicit => {
				if expires_at.is_some() || !self.explicit {
					false
				} else {
					self.explicit = false;
					true
				}
			},
			GrantSource::Implicit => {
				if self.implicit == Some(expires_at) {
					self.implicit = None;
					true
				} else {
					false
				}
			},
			GrantSource::Materialized => {
				if self.materialized == Some(expires_at) {
					self.materialized = None;
					true
				} else {
					false
				}
			},
		}
	}
}

impl GrantSource {
	pub(crate) fn from_i32(value: i32) -> Option<Self> {
		match value {
			0 => Some(Self::Explicit),
			1 => Some(Self::Implicit),
			2 => Some(Self::Materialized),
			_ => None,
		}
	}

	pub(crate) fn to_i32(self) -> i32 {
		match self {
			Self::Explicit => 0,
			Self::Implicit => 1,
			Self::Materialized => 2,
		}
	}
}

impl GrantEntry {
	#[allow(clippy::option_option)]
	pub(crate) fn effective_expires_at(&self) -> Option<Option<i64>> {
		let mut output = None;
		if self.explicit {
			output = Some(None);
		}
		if let Some(expires_at) = self.implicit {
			output = Some(match output {
				Some(output) => max_expires_at(output, expires_at),
				None => expires_at,
			});
		}
		if let Some(expires_at) = self.materialized {
			output = Some(match output {
				Some(output) => max_expires_at(output, expires_at),
				None => expires_at,
			});
		}
		output
	}

	pub(crate) fn has_non_materialized_cover(&self, expires_at: Option<i64>) -> bool {
		self.explicit
			|| self
				.implicit
				.is_some_and(|implicit| max_expires_at(implicit, expires_at) == implicit)
	}

	pub(crate) fn is_non_expiring_process_implicit(&self) -> bool {
		self.implicit == Some(None)
			&& crate::grant::is_process_implicit(
				self.creator.as_ref(),
				self.implicit.is_some(),
				&self.subject,
			)
	}
}

pub(crate) fn max_expires_at(left: Option<i64>, right: Option<i64>) -> Option<i64> {
	match (left, right) {
		(None, _) | (_, None) => None,
		(Some(left), Some(right)) => Some(left.max(right)),
	}
}

#[cfg(test)]
mod tests {
	use super::{GrantSource, GrantValue};

	#[test]
	fn grant_source_ids_are_alphabetical() {
		for (id, source) in [
			GrantSource::Explicit,
			GrantSource::Implicit,
			GrantSource::Materialized,
		]
		.into_iter()
		.enumerate()
		{
			let id = i32::try_from(id).unwrap();
			assert_eq!(source.to_i32(), id);
			assert_eq!(GrantSource::from_i32(id), Some(source));
		}
	}

	#[test]
	fn implicit_grants_upgrade_to_non_expiring() {
		let mut value = GrantValue::default();
		assert!(value.put(GrantSource::Implicit, Some(10), None));
		assert_eq!(value.implicit, Some(Some(10)));
		assert!(value.put(GrantSource::Implicit, None, None));
		assert_eq!(value.implicit, Some(None));
		assert!(!value.put(GrantSource::Implicit, Some(20), None));
		assert_eq!(value.implicit, Some(None));
	}
}
