use tangram_client::prelude::*;

pub mod delete;
pub mod get;
pub mod put;

const MICROSECONDS_PER_SECOND: i64 = 1_000_000;
const STORED_AT_OFFSET: i64 = MICROSECONDS_PER_SECOND - 1;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Entry {
	pub cached_at: i64,
	pub id: tg::object::Id,
	pub partition: u64,
}

pub(crate) fn cached_at_timestamp(cached_at: i64) -> tg::Result<i64> {
	timestamp(cached_at, 0)
}

pub(crate) fn stored_at_timestamp(stored_at: i64) -> tg::Result<i64> {
	timestamp(stored_at, STORED_AT_OFFSET)
}

fn timestamp(value: i64, offset: i64) -> tg::Result<i64> {
	value
		.checked_mul(MICROSECONDS_PER_SECOND)
		.and_then(|value| value.checked_add(offset))
		.ok_or_else(|| tg::error!(%value, "the object timestamp is out of range"))
}

#[cfg(test)]
mod tests {
	#[test]
	fn stored_at_timestamp_follows_cached_at_timestamp() {
		let cached_at = super::cached_at_timestamp(42).unwrap();
		let stored_at = super::stored_at_timestamp(42).unwrap();
		assert!(cached_at < stored_at);
		assert!(stored_at < super::cached_at_timestamp(43).unwrap());
	}
}
