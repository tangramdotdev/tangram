use {
	super::Result,
	crate::quickjs::{StateHandle, serde::Serde},
	futures::{StreamExt as _, TryStreamExt as _, future},
	rquickjs as qjs,
	tangram_client::prelude::*,
};

pub async fn get(
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

pub async fn spawn(
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

pub async fn wait(
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
