use bytes::Bytes;

pub mod create;
pub mod delete;
pub mod get;
pub mod id;
pub mod post;

pub use self::id::Id;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Event {
	Chunk(Bytes),
	End,
}
