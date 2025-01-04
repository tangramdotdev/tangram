use super::State;
use std::rc::Rc;
use tangram_client as tg;

pub async fn output(state: Rc<State>, args: (tg::Target,)) -> tg::Result<tg::Value> {
	let (target,) = args;
	let server = state.server.clone();
	let parent = state.build.clone();
	let remote = state.remote.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move {
			let retry = parent.retry(&server).await?;
			let arg = tg::target::build::Arg {
				create: true,
				parent: Some(parent.id().clone()),
				remote,
				retry,
			};
			target
				.output(&server, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the target output"))
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to build the target"))?;
	Ok(output)
}
