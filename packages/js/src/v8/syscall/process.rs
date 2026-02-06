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

pub async fn spawn(
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

pub async fn wait(
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
