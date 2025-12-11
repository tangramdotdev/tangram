use {super::State, std::rc::Rc, tangram_client::prelude::*, tangram_v8::Serde};

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
	let handle = state.handle.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move { handle.spawn_process(arg).await })
		.await
		.unwrap()?;
	Ok(Serde(output))
}

pub async fn wait(
	state: Rc<State>,
	args: (Serde<tg::process::Id>,),
) -> tg::Result<Serde<tg::process::wait::Output>> {
	let (Serde(id),) = args;
	let handle = state.handle.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move {
			let output = handle
				.wait_process(&id, tg::process::wait::Arg::default())
				.await?;
			Ok::<_, tg::Error>(output)
		})
		.await
		.unwrap()?;
	Ok(Serde(output))
}
