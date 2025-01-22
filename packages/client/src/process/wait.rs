use crate as tg;

pub struct Output {
	pub error: Option<tg::Error>,
	pub exit: Option<tg::process::Exit>,
	pub output: Option<tg::Value>,
	pub status: tg::process::Status,
}
