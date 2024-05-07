use super::State;
use std::rc::Rc;
use tangram_client as tg;

pub async fn output(state: Rc<State>, args: (tg::Target,)) -> tg::Result<tg::Value> {
	let (target,) = args;
	let parent = state.build.id().clone();
	let remote = false;
	let retry = state.build.retry(&state.server).await?;
	let arg = tg::target::build::Arg {
		parent: Some(parent),
		remote,
		retry,
	};
	let output = target
		.output(&state.server, arg)
		.await
		.map_err(|source| tg::error!(!source, %target, "failed to build the target"))?;
	Ok(output)
}
