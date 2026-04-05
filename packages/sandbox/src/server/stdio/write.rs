use {
	crate::{Stdio, server::Server},
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::{pin::pin, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tokio::{io::AsyncWriteExt as _, process::ChildStdin, sync::Mutex},
	tokio_stream::wrappers::ReceiverStream,
};

impl Server {
	pub async fn write_stdio(
		&self,
		id: tg::process::Id,
		arg: crate::client::stdio::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::write::Event>>> {
		if arg.streams.as_slice() != [tg::process::stdio::Stream::Stdin] {
			return Err(tg::error!("expected stdin for a stdio write"));
		}
		let stdin = input_handle(self, &id)?;
		let (sender, receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::process::stdio::write::Event>>(1);
		let task = Task::spawn({
			move |_| async move {
				let mut input = pin!(input);
				while let Some(event) = input.next().await {
					let event = match event {
						Ok(event) => event,
						Err(error) => {
							sender.send(Err(error)).await.ok();
							return;
						},
					};
					match event {
						tg::process::stdio::read::Event::Chunk(chunk) => {
							let result = match &stdin {
								InputHandle::Null => break,
								InputHandle::Pty(pty) => {
									let mut pty = &**pty;
									pty.write_all(&chunk.bytes).await
								},
								InputHandle::Stdin(stdin) => {
									let mut stdin = stdin.lock().await;
									stdin.write_all(&chunk.bytes).await
								},
							};
							if let Err(source) = result {
								sender
									.send(Err(tg::error!(!source, "failed to write stdin")))
									.await
									.ok();
								return;
							}
						},
						tg::process::stdio::read::Event::End => break,
					}
				}
				sender
					.send(Ok(tg::process::stdio::write::Event::End))
					.await
					.ok();
			}
		});
		Ok(ReceiverStream::new(receiver).attach(task).boxed())
	}

	pub(crate) async fn handle_write_stdio_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let id: tg::process::Id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		let arg: crate::client::stdio::Arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or(crate::client::stdio::Arg {
				streams: Vec::new(),
			});
		let input = request
			.sse()
			.map_err(|source| tg::error!(!source, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			})
			.boxed();
		let output = self
			.write_stdio(id, arg, input)
			.await
			.map_err(|source| tg::error!(!source, "failed to handle stdio"))?;
		let stream = output.map(
			|result: tg::Result<tg::process::stdio::write::Event>| match result {
				Ok(event) => event.try_into(),
				Err(error) => error.try_into(),
			},
		);
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::TEXT_EVENT_STREAM.to_string(),
			)
			.body(BoxBody::with_sse_stream(stream))
			.unwrap();
		Ok(response)
	}
}

#[derive(Clone)]
enum InputHandle {
	Null,
	Pty(Arc<crate::common::Pty>),
	Stdin(Arc<Mutex<ChildStdin>>),
}

fn input_handle(server: &Server, id: &tg::process::Id) -> tg::Result<InputHandle> {
	let process = server
		.processes
		.get(id)
		.ok_or_else(|| tg::error!(process = %id, "not found"))?;
	match process.command.stdin {
		Stdio::Null => Ok(InputHandle::Null),
		Stdio::Pipe => process
			.stdin
			.clone()
			.map(InputHandle::Stdin)
			.ok_or_else(|| tg::error!(process = %id, "stdin is not available")),
		Stdio::Tty => process
			.pty
			.clone()
			.map(InputHandle::Pty)
			.ok_or_else(|| tg::error!(process = %id, "stdin is not available")),
	}
}
