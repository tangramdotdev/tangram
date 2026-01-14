use {
	super::State, std::io::Write as _, std::rc::Rc, tangram_client::prelude::*, tangram_v8::Serde,
};

pub fn log(
	state: Rc<State>,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (Serde<tg::process::log::Stream>, String),
) -> tg::Result<()> {
	let (Serde(stream), string) = args;
	let mut writer = Writer::new(state, stream);
	writer
		.write_all(string.as_bytes())
		.map_err(|source| tg::error!(!source, "failed to write log"))?;
	writer
		.flush()
		.map_err(|source| tg::error!(!source, "failed to flush log"))?;
	Ok(())
}

pub(super) struct Writer {
	buf: Vec<u8>,
	state: Rc<State>,
	stream: tg::process::log::Stream,
}

impl Writer {
	pub fn new(state: Rc<State>, stream: tg::process::log::Stream) -> Self {
		Self {
			buf: Vec::with_capacity(4096),
			state,
			stream,
		}
	}
}

impl std::io::Write for Writer {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		self.buf.extend_from_slice(buf);
		if self.buf.len() >= 4096 {
			self.flush()?;
		}
		Ok(buf.len())
	}

	fn flush(&mut self) -> std::io::Result<()> {
		let bytes = std::mem::take(&mut self.buf);
		let stream = self.stream;
		let (sender, receiver) = std::sync::mpsc::channel();
		self.state.main_runtime_handle.spawn({
			let logger = self.state.logger.clone();
			async move {
				let result = (logger)(stream, bytes).await;
				sender.send(result).unwrap();
			}
		});
		receiver.recv().unwrap().map_err(std::io::Error::other)
	}
}
