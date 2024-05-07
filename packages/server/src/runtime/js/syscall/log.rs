use super::State;
use std::rc::Rc;
use tangram_client as tg;

pub fn log(_scope: &mut v8::HandleScope, state: Rc<State>, args: (String,)) -> tg::Result<()> {
	let (string,) = args;
	if let Some(log_sender) = state.log_sender.borrow().as_ref() {
		log_sender.send(string).unwrap();
	}
	Ok(())
}
