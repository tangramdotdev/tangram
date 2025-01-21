use bytes::Bytes;

pub enum Event {
	Bytes(Bytes),
	End,
}
