use super::State;
use std::{rc::Rc, sync::Arc};
use tangram_client::{self as tg, handle::Ext};

pub async fn load(
	state: Rc<State>,
	args: (tg::process::Id, Option<String>),
) -> tg::Result<Arc<tg::process::State>> {
	let (id, remote) = args;
	let server = state.server.clone();
	let state = state
		.main_runtime_handle
		.spawn(async move { tg::Process::new(id, remote, None, None).load(&server).await })
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to load the process"))?;
	Ok(state)
}

pub async fn spawn(
	state: Rc<State>,
	args: (tg::process::spawn::Arg,),
) -> tg::Result<tg::process::spawn::Output> {
	let (arg,) = args;
	let server = state.server.clone();
	let parent = state.process.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move {
			let retry = *parent.retry(&server).await?;
			let arg = tg::process::spawn::Arg {
				create: true,
				parent: Some(parent.id().clone()),
				remote: parent.remote().cloned(),
				retry,
				..arg
			};
			let output = server.spawn_process(arg).await?;
			Ok::<_, tg::Error>(output)
		})
		.await
		.unwrap()?;
	Ok(output)
}

pub async fn wait(state: Rc<State>, args: (tg::process::Id,)) -> tg::Result<tg::process::Wait> {
	let (id,) = args;
	let server = state.server.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move {
			let output = server.wait_process(&id).await?.try_into()?;
			Ok::<_, tg::Error>(output)
		})
		.await
		.unwrap()?;
	Ok(output)
}
