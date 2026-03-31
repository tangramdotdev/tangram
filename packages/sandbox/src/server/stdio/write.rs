use {
	crate::{common::InputStream, server::Server},
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tokio::io::AsyncWriteExt as _,
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
		let (sender, receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::process::stdio::write::Event>>(1);
		let task =
			Task::spawn({
				let server = self.clone();
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
						let stdin = match server.processes.get(&id) {
							Some(process) => process.stdin.clone(),
							None => break,
						};
						let mut stdin = stdin.lock().await;
						match event {
							tg::process::stdio::read::Event::Chunk(chunk) => match &mut *stdin {
								InputStream::Null => break,
								InputStream::Pipe(pipe) => {
									if let Err(error) =
										pipe.write_all(&chunk.bytes).await.map_err(|source| {
											tg::error!(!source, "failed to write stdin")
										}) {
										sender.send(Err(error)).await.ok();
										return;
									}
								},
								InputStream::Pty(pty) => {
									if let Err(error) =
										pty.write_all(&chunk.bytes).await.map_err(|source| {
											tg::error!(!source, "failed to write stdin")
										}) {
										sender.send(Err(error)).await.ok();
										return;
									}
								},
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
