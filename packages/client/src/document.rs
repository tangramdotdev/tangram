/// A document's state.
#[derive(Clone, Debug)]
pub enum State {
	/// A closed document.
	Closed(Closed),

	/// An opened document.
	Opened(Opened),
}

/// A closed document.
#[derive(Clone, Debug)]
pub struct Closed {
	/// The document's version.
	pub version: i32,

	/// The document's last modified time.
	pub modified: std::time::SystemTime,
}

/// An opened document.
#[derive(Clone, Debug)]
pub struct Opened {
	/// The document's version.
	pub version: i32,

	/// The document's text.
	pub text: String,
}
