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
	Temporary,
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

	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub temporary: Option<i64>,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub materialized: Option<Option<i64>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::option_option)]
pub(crate) struct GrantEntry {
	pub explicit: bool,
	pub temporary: Option<i64>,
	pub materialized: Option<Option<i64>>,
	pub permission: tangram_client::grant::Permission,
	pub principal: tangram_client::grant::Principal,
}

#[derive(Clone)]
pub(crate) struct GrantIndexEntry<'a> {
	pub creator: Option<&'a tangram_client::Principal>,
	pub expires_at: Option<i64>,
	pub permission: tangram_client::grant::Permission,
	pub principal: &'a tangram_client::grant::Principal,
	pub resource: &'a tangram_client::Id,
}

impl GrantValue {
	pub(crate) fn deserialize(bytes: &[u8]) -> tangram_client::Result<Self> {
		tangram_serialize::from_slice(bytes).map_err(|error| {
			tangram_client::error!(!error, "failed to deserialize the grant value")
		})
	}

	pub(crate) fn is_empty(&self) -> bool {
		!self.explicit && self.temporary.is_none() && self.materialized.is_none()
	}

	pub(crate) fn serialize(&self) -> tangram_client::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tangram_client::error!(!error, "failed to serialize the grant value"))
	}

	#[allow(clippy::option_option)]
	pub(crate) fn source_expires_at(&self, source: GrantSource) -> Option<Option<i64>> {
		match source {
			GrantSource::Explicit => self.explicit.then_some(None),
			GrantSource::Temporary => self.temporary.map(Some),
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
			GrantSource::Temporary => {
				let Some(expires_at) = expires_at else {
					return false;
				};
				let time_to_touch = time_to_touch
					.map(|value| i64::try_from(value.as_secs()).unwrap())
					.unwrap_or_default();
				if self.temporary.is_some_and(|current| {
					current >= expires_at || expires_at.saturating_sub(current) < time_to_touch
				}) {
					false
				} else {
					self.temporary = Some(expires_at);
					true
				}
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
			GrantSource::Temporary => {
				if self.temporary == expires_at {
					self.temporary = None;
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
			1 => Some(Self::Temporary),
			2 => Some(Self::Materialized),
			_ => None,
		}
	}

	pub(crate) fn to_i32(self) -> i32 {
		match self {
			Self::Explicit => 0,
			Self::Temporary => 1,
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
		if let Some(expires_at) = self.temporary {
			output = Some(match output {
				Some(output) => max_expires_at(output, Some(expires_at)),
				None => Some(expires_at),
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
			|| self.temporary.is_some_and(|temporary| {
				max_expires_at(Some(temporary), expires_at) == Some(temporary)
			})
	}
}

pub(crate) fn max_expires_at(left: Option<i64>, right: Option<i64>) -> Option<i64> {
	match (left, right) {
		(None, _) | (_, None) => None,
		(Some(left), Some(right)) => Some(left.max(right)),
	}
}
