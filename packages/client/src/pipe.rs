use crate::util::serde::BytesBase64;
use bytes::Bytes;
use serde_with::serde_as;

pub mod close;
pub mod create;
pub mod id;
pub mod read;
pub mod write;

pub use self::id::Id;

#[serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum Event {
	Chunk(#[serde_as(as = "BytesBase64")] Bytes),
	End,
}
