use {
	super::Result,
	crate::quickjs::{StateHandle, serde::Serde},
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
		let output = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move { handle.spawn_process(arg).await }
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))??;
		Ok(output)
	}
	.await;
	Result(result.map(Serde))
}

pub async fn wait(
	ctx: qjs::Ctx<'_>,
	id: Serde<tg::process::Id>,
) -> Result<Serde<tg::process::wait::Output>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(id) = id;
	let result = async {
		let output = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let output = handle
						.wait_process(&id, tg::process::wait::Arg::default())
						.await?;
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
