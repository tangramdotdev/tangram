use bytes::Bytes;

pub use self::id::Id;

pub mod close;
pub mod get;
pub mod id;
pub mod open;
pub mod post;

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub window_size: Option<WindowSize>,
}

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct WindowSize {
	pub rows: u16,
	pub cols: u16,
	pub xpos: u16,
	pub ypos: u16,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Event {
	Chunk(Bytes),
	WindowSize(WindowSize),
	End,
}
