use {bytes::Bytes, serde_with::serde_as, tangram_util::serde::BytesBase64};

pub mod read;
pub mod write;

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Chunk {
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,

	pub stream: tangram_client::process::stdio::Stream,
}
