use crate as tg;

pub use event::Event;

pub mod event;

#[derive(Debug)]
pub struct Output {
	pub error: Option<tg::Error>,
	pub exit: Option<tg::process::Exit>,
	pub output: Option<tg::Value>,
	pub status: tg::process::Status,
}
