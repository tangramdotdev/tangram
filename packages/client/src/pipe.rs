use bytes::Bytes;

pub use self::id::Id;

pub mod close;
pub mod id;
pub mod open;
pub mod read;
pub mod write;

#[derive(Debug)]
pub enum Event {
	Chunk(Bytes),
	End,
}
