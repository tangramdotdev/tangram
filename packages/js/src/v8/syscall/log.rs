use {super::State, std::rc::Rc, tangram_client::prelude::*, tangram_v8::Serde};

pub fn log(
	state: Rc<State>,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (Serde<tg::process::log::Stream>, String),
) -> tg::Result<()> {
	let (Serde(stream), string) = args;
	log_inner(state, stream, string.into_bytes())
}

pub(crate) fn log_inner(
	state: Rc<State>,
	stream: tg::process::log::Stream,
	bytes: Vec<u8>,
) -> tg::Result<()> {
	let (sender, receiver) = std::sync::mpsc::channel();
	state.main_runtime_handle.spawn({
		let logger = state.logger.clone();
		async move {
			let result = (logger)(stream, bytes).await;
			sender.send(result).unwrap();
		}
	});
	receiver.recv().unwrap()?;
	Ok(())
}
