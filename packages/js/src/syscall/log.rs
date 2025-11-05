use {super::State, std::rc::Rc, tangram_client::prelude::*, tangram_v8::Serde};

pub fn log(
	state: Rc<State>,
	_scope: &mut v8::HandleScope,
	args: (Serde<tg::process::log::Stream>, String),
) -> tg::Result<()> {
	let (Serde(stream), string) = args;
	let (sender, receiver) = std::sync::mpsc::channel();
	state.main_runtime_handle.spawn({
		let logger = state.logger.clone();
		async move {
			let result = (logger)(stream, string).await;
			sender.send(result).unwrap();
		}
	});
	receiver.recv().unwrap()?;
	Ok(())
}
