use {
	super::Result,
	crate::quickjs::{StateHandle, serde::Serde},
	rquickjs as qjs,
	tangram_client::prelude::*,
};

pub fn log(
	ctx: qjs::Ctx<'_>,
	stream: Serde<tg::process::log::Stream>,
	string: String,
) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(stream) = stream;
	let result = (|| {
		let (sender, receiver) = std::sync::mpsc::channel();
		state.main_runtime_handle.spawn({
			let logger = state.logger.clone();
			async move {
				let result = (logger)(stream, string).await;
				sender.send(result).unwrap();
			}
		});
		receiver
			.recv()
			.map_err(|source| tg::error!(!source, "failed to receive log result"))?
			.map_err(|source| tg::error!(!source, "failed to log"))
	})();
	Result(result)
}
