use super::State;
use std::{pin::pin, rc::Rc, sync::Arc};
use tangram_client::{self as tg, handle::Ext};
use tangram_either::Either;
use tokio_stream::StreamExt as _;

pub async fn load(
	state: Rc<State>,
	args: (tg::process::Id, Option<String>),
) -> tg::Result<Arc<tg::process::State>> {
	let (id, remote) = args;
	let server = state.server.clone();
	let state = state
		.main_runtime_handle
		.spawn(async move {
			let process = tg::Process::new(id, remote, None, None, None);
			process.load(&server).await
		})
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
			// If the parent is remote, then push the command.
			if let Some(remote) = parent.remote() {
				if let Some(command) = &arg.command {
					// Push the command.
					let arg = tg::push::Arg {
						items: vec![Either::Right(command.clone().into())],
						remote: remote.to_owned(),
						..Default::default()
					};
					let stream = server.push(arg).await?;

					// Consume the stream and log progress.
					let mut stream = pin!(stream);
					while let Some(event) = stream.try_next().await? {
						match event {
							tg::progress::Event::Start(indicator)
							| tg::progress::Event::Update(indicator) => {
								if indicator.name == "bytes" {
									let message = format!("{indicator}\n");
									let arg = tg::process::log::post::Arg {
										bytes: message.into(),
										remote: Some(remote.to_owned()),
									};
									server.try_post_process_log(parent.id(), arg).await.ok();
								}
							},
							tg::progress::Event::Output(()) => {
								break;
							},
							_ => {},
						}
					}
				}
			}

			// Spawn.
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
