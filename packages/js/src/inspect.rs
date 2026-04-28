#[derive(Clone, Debug)]
pub struct Options {
	pub addr: Option<std::net::SocketAddr>,
	pub mode: Mode,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Mode {
	#[default]
	Normal,
	Wait,
	Break,
}
