use {
	serde_with::{DisplayFromStr, serde_as},
	tangram_util::serde::is_default,
};

#[serde_as]
#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Debug {
	#[serde_as(as = "Option<DisplayFromStr>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(
		id = 0,
		default,
		serialize_with = "serialize_addr",
		deserialize_with = "deserialize_addr"
	)]
	pub addr: Option<std::net::SocketAddr>,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_default")]
	pub mode: Mode,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	derive_more::Display,
	derive_more::FromStr,
	Eq,
	PartialEq,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[tangram_serialize(display, from_str)]
pub enum Mode {
	#[default]
	Normal,
	Break,
	Wait,
}

#[expect(clippy::ref_option)]
fn serialize_addr(
	value: &Option<std::net::SocketAddr>,
	serializer: &mut tangram_serialize::Serializer<'_>,
) -> std::io::Result<()> {
	let value = value.map(|value| value.to_string());
	serializer.serialize(&value)
}

fn deserialize_addr(
	deserializer: &mut tangram_serialize::Deserializer<'_>,
) -> std::io::Result<Option<std::net::SocketAddr>> {
	deserializer
		.deserialize::<Option<String>>()?
		.map(|value| value.parse().map_err(std::io::Error::other))
		.transpose()
}
