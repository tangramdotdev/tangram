use super::State;
use std::rc::Rc;
use tangram_client::{self as tg, handle::Ext};

pub async fn spawn(
	state: Rc<State>,
	args: (tg::process::spawn::Arg,),
) -> tg::Result<tg::process::Id> {
	// let (arg,) = args;
	// let server = state.server.clone();
	// let parent = state.process.clone();
	// let process = state
	// 	.main_runtime_handle
	// 	.spawn(async move {
	// 		let retry = server.get_process(&parent).await?.retry;
	// 		let arg = tg::process::spawn::Arg {
	// 			create: true,
	// 			parent: Some(parent.clone()),
	// 			remote: Some(parent.remote().clone()),
	// 			retry,
	// 			..arg
	// 		};
	// 		let tg::process::spawn::Output { process, .. } = server.spawn_process(arg).await?;
	// 		Ok(process)
	// 	})
	// 	.await?;
	// Ok(process)
	todo!()
}

pub async fn wait(state: Rc<State>, args: (tg::process::Id)) -> tg::Result<tg::Value> {
	// let server = state.server.clone();
	// let parent = state.process.clone();
	// let remote = state.remote.clone();
	// let stream = server.get_process_wait(&args.process).await?;
	// let Some(tg::process::wait::Event::Output(output)) = pin!(stream).try_last().await? else {
	// 	return Err(tg::error!("failed to wait for the process"));
	// };
	// Ok(output)
	todo!()
}

pub async fn load(
	state: Rc<State>,
	args: (tg::process::Id),
) -> tg::Result<tg::process::get::Output> {
	let server = state.server.clone();
	let parent = state.process.clone();
	let remote = state.remote.clone();
	let output = server.get_process(id).await?;
	Ok(output)
}
