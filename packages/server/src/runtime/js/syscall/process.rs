use super::State;
use std::{pin::pin, rc::Rc, sync::Arc};
use tangram_client::{self as tg, handle::Ext};
use tokio_stream::StreamExt as _;

pub async fn load(
	state: Rc<State>,
	args: (tg::process::Id, Option<String>),
) -> tg::Result<Arc<tg::process::State>> {
	let (id, remote) = args;
	let server = state.server.clone();
	let state = state
		.main_runtime_handle
		.spawn(async move { tg::Process::new(id, remote, None, None).load(&server).await })
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
			// If the parent remote set, push the command.
			if let Some(remote) = parent.remote() {
				if let Some(ref command) = arg.command {
					let client = server.get_remote_client(remote.to_string()).await?;

					let arg = tg::object::push::Arg {
						remote: remote.to_owned(),
					};
					let stream = server.push_object(&command.clone().into(), arg).await?;
					let mut stream = pin!(stream);
					// Log that the push started.
					let message = format!("pushing {command}\n");
					let arg = tg::process::log::post::Arg {
						bytes: message.into(),
						remote: Some(remote.to_owned()),
					};
					client.try_post_process_log(parent.id(), arg).await.ok();

					// Consume the stream.
					while let Some(result) = stream.next().await {
						let event = match result {
							Ok(event) => event,
							Err(error) => return Err(error),
						};

						// Handle the event.
						match event {
							tg::progress::Event::Start(indicator)
							| tg::progress::Event::Update(indicator) => {
								if indicator.name == "bytes" {
									let message = format!("{indicator}\n");
									let arg = tg::process::log::post::Arg {
										bytes: message.into(),
										remote: Some(remote.to_owned()),
									};
									client.try_post_process_log(parent.id(), arg).await.ok();
								}
							},
							tg::progress::Event::Output(()) => {
								break;
							},
							_ => {},
						}
					}

					// Log that the push finished.
					let message = format!("finished pushing {command}\n");
					let arg = tg::process::log::post::Arg {
						bytes: message.into(),
						remote: Some(remote.to_owned()),
					};
					client.try_post_process_log(parent.id(), arg).await.ok();
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

			// let output = if let Some(remote) = parent.remote() {
			// 	let client = server.get_remote_client(remote.to_string()).await?;
			// 	client
			// 		.spawn_process(tg::process::spawn::Arg {
			// 			remote: None,
			// 			..arg
			// 		})
			// 		.await?
			// 	// client.spawn_process(arg).await?
			// } else {
			// server.spawn_process(arg).await?
			// };

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
