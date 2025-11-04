use {
	super::State,
	futures::TryStreamExt as _,
	std::{pin::pin, rc::Rc},
	tangram_client::{self as tg, prelude::*},
	tangram_either::Either,
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
	let handle = state.handle.clone();
	let parent = state.process.clone();
	let logger = state.logger.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move {
			// If the parent is remote, then push the command.
			if let Some(remote) = parent.remote() {
				// Push the command.
				let arg = tg::push::Arg {
					items: vec![Either::Right(arg.command.item.clone().into())],
					remote: Some(remote.to_owned()),
					..Default::default()
				};
				let stream = handle.push(arg).await?;

				// Consume the stream and log progress.
				let mut stream = pin!(stream);
				while let Some(event) = stream.try_next().await? {
					match event {
						tg::progress::Event::Start(indicator)
						| tg::progress::Event::Update(indicator) => {
							if indicator.name == "bytes" {
								let message = format!("{indicator}\n");
								(logger)(tg::process::log::Stream::Stderr, message).await?;
							}
						},
						tg::progress::Event::Output(_) => {
							break;
						},
						_ => (),
					}
				}
			}

			// Spawn.
			let retry = *parent.retry(&handle).await?;
			let arg = tg::process::spawn::Arg {
				parent: Some(parent.id().clone()),
				remote: parent.remote().cloned(),
				retry,
				..arg
			};
			let output = handle.spawn_process(arg).await?;

			Ok::<_, tg::Error>(output)
		})
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
			let output = handle.wait_process(&id).await?;
			Ok::<_, tg::Error>(output)
		})
		.await
		.unwrap()?;
	Ok(Serde(output))
}
