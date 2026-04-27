#[derive(Clone, Debug)]
pub struct Options {
	pub addr: Option<std::net::SocketAddr>,
	pub mode: Mode,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum Mode {
	#[default]
	Normal,
	Wait,
	Break,
}
