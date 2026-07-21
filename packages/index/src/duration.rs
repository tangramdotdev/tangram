use std::time::Duration;

pub(super) fn deserialize(
	deserializer: &mut tangram_serialize::Deserializer<'_>,
) -> std::io::Result<Duration> {
	let (seconds, nanoseconds) = deserializer.deserialize::<(u64, u32)>()?;
	if nanoseconds >= 1_000_000_000 {
		return Err(std::io::Error::other("invalid duration nanoseconds"));
	}
	Ok(Duration::new(seconds, nanoseconds))
}

pub(super) fn deserialize_option(
	deserializer: &mut tangram_serialize::Deserializer<'_>,
) -> std::io::Result<Option<Duration>> {
	let value = deserializer.deserialize::<Option<(u64, u32)>>()?;
	value
		.map(|(seconds, nanoseconds)| {
			if nanoseconds >= 1_000_000_000 {
				return Err(std::io::Error::other("invalid duration nanoseconds"));
			}
			Ok(Duration::new(seconds, nanoseconds))
		})
		.transpose()
}

pub(super) fn serialize(
	value: &Duration,
	serializer: &mut tangram_serialize::Serializer<'_>,
) -> std::io::Result<()> {
	serializer.serialize(&(value.as_secs(), value.subsec_nanos()))
}

#[expect(clippy::ref_option)]
pub(super) fn serialize_option(
	value: &Option<Duration>,
	serializer: &mut tangram_serialize::Serializer<'_>,
) -> std::io::Result<()> {
	let value = value.map(|value| (value.as_secs(), value.subsec_nanos()));
	serializer.serialize(&value)
}
