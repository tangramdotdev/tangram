use {serde_with::serde_as, tangram_client::prelude::*, tangram_util::serde::CommaSeparatedString};

pub mod read;
pub mod write;

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde_as(as = "CommaSeparatedString")]
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub streams: Vec<tg::process::stdio::Stream>,
}
