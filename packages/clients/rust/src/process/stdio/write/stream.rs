use {
	super::super::Stream, crate::prelude::*, serde_with::serde_as,
	tangram_util::serde::CommaSeparatedString,
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
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[serde_as(as = "CommaSeparatedString")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Vec::is_empty")]
	pub streams: Vec<Stream>,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	#[tangram_serialize(
		default,
		id = 2,
		skip_serializing_if = "tg::authorization::Tokens::is_empty"
	)]
	pub tokens: tg::authorization::Tokens,
}
