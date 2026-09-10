/// An event from a reconnecting control stream.
#[derive(Clone, Debug)]
pub enum Event<T> {
	Message(T),
	Reconnect,
}
