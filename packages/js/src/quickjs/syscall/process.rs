use {
	super::Result,
	crate::quickjs::{StateHandle, serde::Serde},
	futures::{StreamExt as _, TryStreamExt as _, future},
	rquickjs as qjs,
	tangram_client::prelude::*,
};

pub(super) async fn get(
	ctx: qjs::Ctx<'_>,
	id: Serde<tg::process::Id>,
) -> Result<Serde<tg::process::Data>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(id) = id;
	let result = async {
		let data = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				let id = id.clone();
				async move {
					let tg::process::get::Output { data, .. } = handle.get_process(&id).await?;
					Ok::<_, tg::Error>(data)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to get the process"))?;
		Ok(data)
	}
	.await;
	Result(result.map(Serde))
}

pub(super) async fn spawn_sandboxed(
	ctx: qjs::Ctx<'_>,
	arg: Serde<tg::process::spawn::Arg>,
) -> Result<Serde<tg::process::spawn::Output>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(arg) = arg;
	let result = async {
		let stream = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let stream = handle
						.try_spawn_process(arg)
						.await?
						.and_then(|event| {
							let result = event.try_map_output(
								|output: Option<tg::process::spawn::Output>| {
									output.ok_or_else(|| tg::error!("expected a process"))
								},
							);
							future::ready(result)
						})
						.boxed();
					Ok::<_, tg::Error>(stream)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))??;
		let writer = super::log::Writer::new(state.clone(), tg::process::log::Stream::Stderr);
		let handle = state.handle.clone();
		let output = tg::progress::write_progress_stream(&handle, stream, writer, false).await?;
		Ok(output)
	}
	.await;
	Result(result.map(Serde))
}

pub(super) async fn spawn_unsandboxed(
	ctx: qjs::Ctx<'_>,
	arg: Serde<tg::process::spawn::Arg>,
) -> Result<Serde<i32>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(arg) = arg;
	let result = async {
		let handle = state.handle.clone();
		let child_process = state
			.main_runtime_handle
			.spawn(async move { crate::process::spawn_unsandboxed(&handle, arg).await })
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
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

		Ok(pid)
	}
	.await;
	Result(result.map(Serde))
}

pub(super) async fn wait_sandboxed(
	ctx: qjs::Ctx<'_>,
	id: Serde<tg::process::Id>,
	arg: Serde<tg::process::wait::Arg>,
) -> Result<Serde<tg::process::wait::Output>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(id) = id;
	let Serde(arg) = arg;
	let result = async {
		let output = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let output = handle.wait_process(&id, arg).await?;
					Ok::<_, tg::Error>(output)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))??;
		Ok(output)
	}
	.await;
	Result(result.map(Serde))
}

pub(super) async fn wait_unsandboxed(
	ctx: qjs::Ctx<'_>,
	pid: Serde<i32>,
) -> Result<Serde<crate::process::WaitOutput>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(pid) = pid;
	let result = async {
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
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to wait for the unsandboxed process"))?;

		Ok(output)
	}
	.await;
	Result(result.map(Serde))
}
