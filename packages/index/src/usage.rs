use tangram_client::prelude::*;

pub use tangram_client::usage::{Account, Aggregate, Period, PeriodKind};

pub mod aggregate;
pub mod compute;
pub mod expire;
pub mod storage;

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	num_derive::FromPrimitive,
	num_derive::ToPrimitive,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[repr(u8)]
pub enum DeltaKind {
	#[tangram_serialize(id = 0)]
	ObjectCount = 0,

	#[tangram_serialize(id = 1)]
	ObjectSize = 1,

	#[tangram_serialize(id = 2)]
	ProcessCount = 2,

	#[tangram_serialize(id = 3)]
	SandboxCount = 3,

	#[tangram_serialize(id = 4)]
	SandboxCpu = 4,

	#[tangram_serialize(id = 5)]
	SandboxMemory = 5,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PartitionAggregate {
	pub object_count: i128,
	pub object_size: i128,
	pub process_count: i128,
	pub sandbox_count: i128,
	pub sandbox_cpu: u128,
	pub sandbox_memory: u128,
}

#[derive(Clone, Copy)]
pub(crate) struct DeltaArg<'a> {
	pub account: &'a Account,
	pub at: i64,
	pub delta: i64,
	pub kind: DeltaKind,
	pub partition: u64,
}

pub(crate) fn deserialize_timestamp(bytes: &[u8]) -> tg::Result<i64> {
	let bytes = bytes
		.try_into()
		.map_err(|_| tg::error!("invalid usage timestamp"))?;
	let value = u64::from_le_bytes(bytes) ^ (1 << 63);
	let timestamp = value.cast_signed();

	Ok(timestamp)
}

pub(crate) fn serialize_timestamp(timestamp: i64) -> [u8; 8] {
	(timestamp.cast_unsigned() ^ (1 << 63)).to_le_bytes()
}

impl PartitionAggregate {
	pub fn checked_add(&mut self, other: Self) -> tg::Result<()> {
		self.object_count = self
			.object_count
			.checked_add(other.object_count)
			.ok_or_else(|| tg::error!("the object count usage overflowed"))?;
		self.object_size = self
			.object_size
			.checked_add(other.object_size)
			.ok_or_else(|| tg::error!("the object size usage overflowed"))?;
		self.process_count = self
			.process_count
			.checked_add(other.process_count)
			.ok_or_else(|| tg::error!("the process count usage overflowed"))?;
		self.sandbox_count = self
			.sandbox_count
			.checked_add(other.sandbox_count)
			.ok_or_else(|| tg::error!("the sandbox count usage overflowed"))?;
		self.sandbox_cpu = self
			.sandbox_cpu
			.checked_add(other.sandbox_cpu)
			.ok_or_else(|| tg::error!("the sandbox CPU usage overflowed"))?;
		self.sandbox_memory = self
			.sandbox_memory
			.checked_add(other.sandbox_memory)
			.ok_or_else(|| tg::error!("the sandbox memory usage overflowed"))?;

		Ok(())
	}

	pub fn try_into_aggregate(self) -> tg::Result<Aggregate> {
		let object_count = u64::try_from(self.object_count)
			.map_err(|_| tg::error!("the object count usage is out of range"))?;
		let object_size = u64::try_from(self.object_size)
			.map_err(|_| tg::error!("the object size usage is out of range"))?;
		let process_count = u64::try_from(self.process_count)
			.map_err(|_| tg::error!("the process count usage is out of range"))?;
		let sandbox_count = u64::try_from(self.sandbox_count)
			.map_err(|_| tg::error!("the sandbox count usage is out of range"))?;
		let aggregate = Aggregate {
			object_count,
			object_size,
			process_count,
			sandbox_count,
			sandbox_cpu: self.sandbox_cpu,
			sandbox_memory: self.sandbox_memory,
		};

		Ok(aggregate)
	}
}

pub(crate) fn children(period: Period) -> tg::Result<Vec<Period>> {
	let (kind, step) = match period {
		Period::Day(_) => (PeriodKind::Hour, 60 * 60),
		Period::Hour(_) => return Ok(Vec::new()),
		Period::Month(_) | Period::Week(_) => (PeriodKind::Day, 24 * 60 * 60),
	};
	let mut children = Vec::new();
	let mut start = period.start().as_second();
	let end = period.end().as_second();
	while start < end {
		children.push(Period::from_kind_and_start(kind, start)?);
		start = start
			.checked_add(step)
			.ok_or_else(|| tg::error!("the usage period overflowed"))?;
	}

	Ok(children)
}

pub(crate) fn closing_hour(period: Period) -> tg::Result<i64> {
	period
		.end()
		.as_second()
		.checked_sub(60 * 60)
		.ok_or_else(|| tg::error!("the usage period overflowed"))
}

pub(crate) fn deserialize_aggregate(bytes: &[u8]) -> tg::Result<PartitionAggregate> {
	let bytes: &[u8; 96] = bytes
		.try_into()
		.map_err(|_| tg::error!("invalid usage aggregate"))?;
	let sandbox_cpu = u128::from_le_bytes(bytes[0..16].try_into().unwrap());
	let sandbox_memory = u128::from_le_bytes(bytes[16..32].try_into().unwrap());
	let object_count = i128::from_le_bytes(bytes[32..48].try_into().unwrap());
	let object_size = i128::from_le_bytes(bytes[48..64].try_into().unwrap());
	let process_count = i128::from_le_bytes(bytes[64..80].try_into().unwrap());
	let sandbox_count = i128::from_le_bytes(bytes[80..96].try_into().unwrap());
	let aggregate = PartitionAggregate {
		object_count,
		object_size,
		process_count,
		sandbox_count,
		sandbox_cpu,
		sandbox_memory,
	};

	Ok(aggregate)
}

pub(crate) fn serialize_aggregate(aggregate: &PartitionAggregate) -> Vec<u8> {
	let mut bytes = Vec::with_capacity(96);
	bytes.extend(aggregate.sandbox_cpu.to_le_bytes());
	bytes.extend(aggregate.sandbox_memory.to_le_bytes());
	bytes.extend(aggregate.object_count.to_le_bytes());
	bytes.extend(aggregate.object_size.to_le_bytes());
	bytes.extend(aggregate.process_count.to_le_bytes());
	bytes.extend(aggregate.sandbox_count.to_le_bytes());

	bytes
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn aggregate_serialization_preserves_u128_values() {
		let expected = PartitionAggregate {
			object_count: -1,
			object_size: -2,
			process_count: -3,
			sandbox_count: -4,
			sandbox_cpu: u128::MAX,
			sandbox_memory: u128::from(u64::MAX) + 1,
		};
		let bytes = serialize_aggregate(&expected);
		assert_eq!(bytes.len(), 96);
		let actual = deserialize_aggregate(&bytes).unwrap();
		assert_eq!(actual, expected);
	}

	#[test]
	fn timestamp_serialization_preserves_order() {
		let timestamps = [i64::MIN, -1, 0, 1, i64::MAX];
		let values = timestamps.map(serialize_timestamp);
		for (left, right) in values.iter().zip(values.iter().skip(1)) {
			assert!(u64::from_le_bytes(*left) < u64::from_le_bytes(*right));
		}
		for (timestamp, value) in std::iter::zip(timestamps, values) {
			assert_eq!(deserialize_timestamp(&value).unwrap(), timestamp);
		}
	}
}
