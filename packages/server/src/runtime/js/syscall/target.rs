use super::State;
use std::rc::Rc;
use tangram_client as tg;

pub async fn output(state: Rc<State>, args: (tg::Target,)) -> tg::Result<tg::Value> {
	let (target,) = args;
	let server = state.server.clone();
	let parent = state.build.id().clone();
	let remote = state.remote.clone().map(either::Either::Right);
	let retry = state.build.retry(&state.server).await?;
	let arg = tg::target::build::Arg {
		parent: Some(parent),
		remote,
		retry,
	};
	let output = state
		.main_runtime_handle
		.spawn(async move { target.output(&server, arg).await })
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to build the target"))?;
	Ok(output)
}
