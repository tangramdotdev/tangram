use super::State;
use futures::FutureExt as _;
use std::rc::Rc;
use tangram_client::{self as tg, handle::Ext};

pub async fn output(state: Rc<State>, args: (tg::Command,)) -> tg::Result<tg::Value> {
	let (command,) = args;
	let server = state.server.clone();
	let parent = state.process.clone();
	let remote = state.remote.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move {
			let retry = server.get_process(&parent).await?.retry;
			let arg = tg::command::spawn::Arg {
				create: true,
				parent: Some(parent.clone()),
				remote,
				retry,
			};
			command
				.output(&server, arg)
				.boxed()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the command's output"))
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to get the command's output"))?;
	Ok(output)
}
