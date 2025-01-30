use super::State;
use std::rc::Rc;
use tangram_client as tg;

pub async fn spawn(
	state: Rc<State>,
	args: (tg::process::spawn::Arg,),
) -> tg::Result<tg::process::Id> {
	let (arg,) = args;
	let server = state.server.clone();
	let parent = state.process.clone();
	let process = state
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
			let process = tg::Process::spawn(&server, arg).await?;
			Ok::<_, tg::Error>(process)
		})
		.await
		.unwrap()?;
	Ok(process.id().clone())
}

pub async fn wait(
	state: Rc<State>,
	args: (tg::process::Id,),
) -> tg::Result<tg::process::wait::Output> {
	let (id,) = args;
	let server = state.server.clone();
	let parent = state.process.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move {
			let output = tg::Process::new(id, parent.remote().cloned(), None)
				.wait(&server)
				.await?;
			Ok::<_, tg::Error>(output)
		})
		.await
		.unwrap()?;
	Ok(output)
}
