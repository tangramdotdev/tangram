use super::State;
use crate::runtime::util;
use futures::TryStreamExt as _;
use std::{pin::pin, rc::Rc};
use tangram_client::{self as tg, prelude::*};
use tangram_either::Either;
use tangram_v8::Serde;

pub async fn get(
	state: Rc<State>,
	args: (Serde<tg::process::Id>, Option<String>),
) -> tg::Result<Serde<tg::process::Data>> {
	let (Serde(id), _) = args;
	let server = state.server.clone();
	let data = state
		.main_runtime_handle
		.spawn(async move {
			let tg::process::get::Output { data } = server.get_process(&id).await?;
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
						items: vec![Either::Right(command.item.clone().into())],
						remote: Some(remote.to_owned()),
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
									util::log(
										&server,
										&parent,
										tg::process::log::Stream::Stderr,
										message,
									)
									.await;
								}
							},
							tg::progress::Event::Output(_) => {
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
	Ok(Serde(output))
}

pub async fn wait(
	state: Rc<State>,
	args: (Serde<tg::process::Id>,),
) -> tg::Result<Serde<tg::process::wait::Output>> {
	let (Serde(id),) = args;
	let server = state.server.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move {
			let output = server.wait_process(&id).await?;
			Ok::<_, tg::Error>(output)
		})
		.await
		.unwrap()?;
	Ok(Serde(output))
}
