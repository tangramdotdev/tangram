use bytes::Bytes;

pub use self::id::Id;

pub mod create;
pub mod delete;
pub mod id;
pub mod read;
pub mod write;

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub size: Size,
}

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Size {
	pub rows: u16,
	pub cols: u16,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Event {
	Chunk(Bytes),
	Size(Size),
	End,
}
