use bytes::Bytes;

pub mod create;
pub mod delete;
pub mod id;
pub mod read;
pub mod write;

pub use self::id::Id;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Event {
	Chunk(Bytes),
	End,
}
