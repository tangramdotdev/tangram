use {
	super::State,
	futures::{StreamExt as _, TryStreamExt as _, future},
	std::rc::Rc,
	tangram_client::prelude::*,
	tangram_v8::Serde,
};

pub async fn get(
	state: Rc<State>,
	args: (Serde<tg::process::Id>, Option<String>),
) -> tg::Result<Serde<tg::process::Data>> {
	let (Serde(id), _) = args;
	let handle = state.handle.clone();
	let data = state
		.main_runtime_handle
		.spawn(async move {
			let tg::process::get::Output { data, .. } = handle.get_process(&id).await?;
			Ok::<_, tg::Error>(data)
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to get the process"))?;
	Ok(Serde(data))
}

pub async fn spawn_sandboxed(
	state: Rc<State>,
	args: (Serde<tg::process::spawn::Arg>,),
) -> tg::Result<Serde<tg::process::spawn::Output>> {
	let (Serde(arg),) = args;
	let stream = state
		.main_runtime_handle
		.spawn({
			let handle = state.handle.clone();
			async move {
				let stream = handle
					.try_spawn_process(arg)
					.await?
					.and_then(|event| {
						let result =
							event.try_map_output(|output: Option<tg::process::spawn::Output>| {
								output.ok_or_else(|| tg::error!("expected a process"))
							});
						future::ready(result)
					})
					.boxed();
				Ok::<_, tg::Error>(stream)
			}
		})
		.await
		.unwrap()?;
	let writer = super::log::Writer::new(state.clone(), tg::process::log::Stream::Stderr);
	let handle = state.handle.clone();
	let output = tg::progress::write_progress_stream(&handle, stream, writer, false).await?;
	Ok(Serde(output))
}

pub async fn spawn_unsandboxed(
	state: Rc<State>,
	args: (Serde<tg::process::spawn::Arg>,),
) -> tg::Result<Serde<i32>> {
	let (Serde(arg),) = args;
	let handle = state.handle.clone();
	let child_process = state
		.main_runtime_handle
		.spawn(async move { crate::process::spawn_unsandboxed(&handle, arg).await })
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to spawn the unsandboxed process"))?;

	// Get the PID.
	#[allow(clippy::cast_possible_wrap)]
	let pid = child_process
		.child
		.id()
		.ok_or_else(|| tg::error!("failed to get the child process ID"))?
		.cast_signed();

	// Store the child process in state.
	state.children.borrow_mut().insert(pid, child_process);

	Ok(Serde(pid))
}

pub async fn wait_sandboxed(
	state: Rc<State>,
	args: (Serde<tg::process::Id>, Serde<tg::process::wait::Arg>),
) -> tg::Result<Serde<tg::process::wait::Output>> {
	let (Serde(id), Serde(arg)) = args;
	let handle = state.handle.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move {
			let output = handle.wait_process(&id, arg).await?;
			Ok::<_, tg::Error>(output)
		})
		.await
		.unwrap()?;
	Ok(Serde(output))
}

pub async fn wait_unsandboxed(
	state: Rc<State>,
	args: (Serde<i32>,),
) -> tg::Result<Serde<crate::process::WaitOutput>> {
	let (Serde(pid),) = args;

	// Remove the child process from state.
	let child_process = state
		.children
		.borrow_mut()
		.remove(&pid)
		.ok_or_else(|| tg::error!(%pid, "unknown child process"))?;

	// Wait for the child process to complete.
	let handle = state.handle.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move { crate::process::wait_unsandboxed(&handle, child_process).await })
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to wait for the unsandboxed process"))?;

	Ok(Serde(output))
}
