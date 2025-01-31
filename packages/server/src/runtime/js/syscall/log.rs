use super::State;
use std::{rc::Rc, str::FromStr};
use tangram_client as tg;

#[derive(Debug)]
pub struct Message {
	pub contents: String,
	pub level: Level,
}

#[derive(Debug)]
pub enum Level {
	Log,
	Error,
}

pub fn log(
	_scope: &mut v8::HandleScope,
	state: Rc<State>,
	args: (String, String),
) -> tg::Result<()> {
	let (contents, level) = args;
	let message = Message {
		contents,
		level: level.parse()?,
	};
	if let Some(log_sender) = state.log_sender.borrow().as_ref() {
		log_sender.send(message).unwrap();
	}
	Ok(())
}

impl FromStr for Level {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"log" => Ok(Level::Log),
			"error" => Ok(Level::Error),
			level => Err(tg::error!(%level, "expected a log level")),
		}
	}
}
