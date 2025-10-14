use {crate::util::serde::BytesBase64, bytes::Bytes, serde_with::serde_as};

pub use self::{id::Id, size::Size};

pub mod close;
pub mod create;
pub mod delete;
pub mod id;
pub mod read;
pub mod size;
pub mod write;

#[serde_as]
#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum Event {
	Chunk(#[serde_as(as = "BytesBase64")] Bytes),
	Size(Size),
	End,
}
