use {super::State, crate::runtime::util, std::rc::Rc, tangram_client as tg, tangram_v8::Serde};

pub fn log(
	state: Rc<State>,
	_scope: &mut v8::HandleScope,
	args: (Serde<tg::process::log::Stream>, String),
) -> tg::Result<()> {
	let (Serde(stream), string) = args;
	let (sender, receiver) = std::sync::mpsc::channel();
	state.main_runtime_handle.spawn({
		let server = state.server.clone();
		let process = state.process.clone();
		async move {
			util::log(&server, &process, stream, string).await;
			sender.send(()).unwrap();
		}
	});
	receiver.recv().unwrap();
	Ok(())
}
