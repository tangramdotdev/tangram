use super::State;
use std::rc::Rc;
use tangram_client as tg;
use tangram_v8::FromV8;

pub struct Message {
	pub stream: Stream,
	pub string: String,
}

pub enum Stream {
	Stdout,
	Stderr,
}

pub fn log(
	state: Rc<State>,
	_scope: &mut v8::HandleScope,
	args: (Stream, String),
) -> tg::Result<()> {
	let (stream, string) = args;
	let message = Message { stream, string };
	if let Some(log_sender) = state.log_sender.borrow().as_ref() {
		log_sender.send(message).unwrap();
	}
	Ok(())
}

impl FromV8 for Stream {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl std::fmt::Display for Stream {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Stdout => write!(f, "stdout"),
			Self::Stderr => write!(f, "stderr"),
		}
	}
}

impl std::str::FromStr for Stream {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"stdout" => Ok(Stream::Stdout),
			"stderr" => Ok(Stream::Stderr),
			stream => Err(tg::error!(%stream, "invalid stream")),
		}
	}
}
