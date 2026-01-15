use {
	crate::{Context, Server},
	futures::{prelude::*, stream::FuturesUnordered},
	std::{
		panic::AssertUnwindSafe,
		pin::pin,
		sync::{Arc, Mutex},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{Body, request::Ext as _},
	tokio_stream::wrappers::ReceiverStream,
};

impl Server {
	pub(crate) async fn push_with_context(
		&self,
		_context: &Context,
		arg: tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + use<>,
	> {
		let remote = arg.remote.clone().unwrap_or_else(|| "default".to_owned());
		let remote = self
			.get_remote_client(remote)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remote client"))?;
		Self::push_or_pull(self, &remote, &arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the push"))
	}

	pub(crate) async fn push_or_pull<S, D>(
		src: &S,
		dst: &D,
		arg: &tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + use<S, D>,
	>
	where
		S: tg::Handle,
		D: tg::Handle,
	{
		// Create the progress handle and add the indicators.
		let progress = crate::progress::Handle::new();
		if arg.items.iter().any(tg::Either::is_right) {
			progress.start(
				"processes".to_owned(),
				"processes".to_owned(),
				tg::progress::IndicatorFormat::Normal,
				Some(0),
				None,
			);
		}
		progress.start(
			"objects".to_owned(),
			"objects".to_owned(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		progress.start(
			"bytes".to_owned(),
			"bytes".to_owned(),
			tg::progress::IndicatorFormat::Bytes,
			Some(0),
			None,
		);

		// Spawn a task to set the indicator totals as soon as they are ready.
		let indicator_total_task = Task::spawn({
			let src = src.clone();
			let progress = progress.clone();
			let arg = arg.clone();
			|_| async move { Self::push_or_pull_set_indicator_totals(&src, progress, &arg).await }
		});

		// Spawn the task.
		let task = Task::spawn({
			let progress = progress.clone();
			let arg = arg.clone();
			let src = src.clone();
			let dst = dst.clone();
			|_| async move {
				let result =
					AssertUnwindSafe(Self::push_or_pull_task(arg, progress.clone(), src, dst))
						.catch_unwind()
						.await;
				match result {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						progress.error(tg::error!(?message, "the task panicked"));
					},
				}
			}
		});

		// Create the stream.
		let stream = progress.stream().attach(indicator_total_task).attach(task);

		Ok(stream)
	}

	async fn push_or_pull_set_indicator_totals<S>(
		src: &S,
		progress: crate::progress::Handle<tg::push::Output>,
		arg: &tg::push::Arg,
	) -> tg::Result<()>
	where
		S: tg::Handle,
	{
		let mut metadata_futures = arg
			.items
			.iter()
			.map(|item| {
				let src = src.clone();
				async move {
					loop {
						match item {
							tg::Either::Left(object) => {
								let metadata_arg = tg::object::metadata::Arg::default();
								let metadata = src
									.try_get_object_metadata(object, metadata_arg)
									.await?
									.ok_or_else(|| tg::error!("expected the metadata to be set"))?;
								if metadata.subtree.count.is_some()
									&& metadata.subtree.size.is_some()
								{
									break Ok::<_, tg::Error>(tg::Either::Left(metadata));
								}
							},
							tg::Either::Right(process) => {
								let metadata_arg = tg::process::metadata::Arg::default();
								let Some(metadata) = src
									.try_get_process_metadata(process, metadata_arg)
									.await
									.map_err(|source| {
										tg::error!(!source, "failed to get the process")
									})?
								else {
									return Err(tg::error!("failed to get the process"));
								};
								let mut stored = true;
								if arg.recursive {
									stored = stored && metadata.subtree.count.is_some();
									if arg.commands {
										stored = stored
											&& metadata.subtree.command.count.is_some()
											&& metadata.subtree.command.size.is_some();
									}
									if arg.outputs {
										stored = stored
											&& metadata.subtree.output.count.is_some()
											&& metadata.subtree.output.size.is_some();
									}
								} else {
									if arg.commands {
										stored = stored
											&& metadata.node.command.count.is_some()
											&& metadata.node.command.size.is_some();
									}
									if arg.outputs {
										stored = stored
											&& metadata.node.output.count.is_some()
											&& metadata.node.output.size.is_some();
									}
								}
								if stored {
									break Ok::<_, tg::Error>(tg::Either::Right(metadata));
								}
							},
						}
						tokio::time::sleep(Duration::from_secs(1)).await;
					}
				}
			})
			.collect::<FuturesUnordered<_>>();
		let mut processes: Option<u64> = None;
		let mut objects: Option<u64> = None;
		let mut bytes: Option<u64> = None;
		while let Some(Ok(metadata)) = metadata_futures.next().await {
			match metadata {
				tg::Either::Left(metadata) => {
					if let Some(count) = metadata.subtree.count {
						*objects.get_or_insert(0) += count;
					}
					if let Some(size) = metadata.subtree.size {
						*bytes.get_or_insert(0) += size;
					}
				},
				tg::Either::Right(metadata) => {
					if arg.recursive {
						if let Some(count) = metadata.subtree.count {
							*processes.get_or_insert(0) += count;
						}
						if arg.commands {
							if let Some(commands_count) = metadata.subtree.command.count {
								*objects.get_or_insert(0) += commands_count;
							}
							if let Some(commands_size) = metadata.subtree.command.size {
								*bytes.get_or_insert(0) += commands_size;
							}
						}
						if arg.outputs {
							if let Some(outputs_count) = metadata.subtree.output.count {
								*objects.get_or_insert(0) += outputs_count;
							}
							if let Some(outputs_size) = metadata.subtree.output.size {
								*bytes.get_or_insert(0) += outputs_size;
							}
						}
					} else {
						if arg.commands {
							if let Some(command_count) = metadata.node.command.count {
								*objects.get_or_insert(0) += command_count;
							}
							if let Some(command_size) = metadata.node.command.size {
								*bytes.get_or_insert(0) += command_size;
							}
						}
						if arg.outputs {
							if let Some(output_count) = metadata.node.output.count {
								*objects.get_or_insert(0) += output_count;
							}
							if let Some(output_size) = metadata.node.output.size {
								*bytes.get_or_insert(0) += output_size;
							}
						}
					}
				},
			}
			progress.set_total("processes", processes);
			progress.set_total("objects", objects);
			progress.set_total("bytes", bytes);
		}
		Ok(())
	}

	async fn push_or_pull_task<S, D>(
		arg: tg::push::Arg,
		progress: crate::progress::Handle<tg::push::Output>,
		src: S,
		dst: D,
	) -> tg::Result<tg::push::Output>
	where
		S: tg::Handle,
		D: tg::Handle,
	{
		let output = Arc::new(Mutex::new(tg::push::Output::default()));

		// Set the progress to zero.
		progress.set("processes", 0);
		progress.set("objects", 0);
		progress.set("bytes", 0);

		// Create the channels.
		let (push_output_sender, push_output_receiver) = tokio::sync::mpsc::channel(1024);
		let (pull_output_sender, pull_output_receiver) = tokio::sync::mpsc::channel(1024);

		// Start the push.
		let push_arg = tg::sync::Arg {
			commands: arg.commands,
			errors: arg.errors,
			eager: arg.eager,
			force: arg.force,
			get: Vec::new(),
			local: None,
			logs: arg.logs,
			metadata: arg.metadata,
			outputs: arg.outputs,
			put: Vec::new(),
			recursive: arg.recursive,
			remotes: None,
		};
		let push_input_stream = ReceiverStream::new(pull_output_receiver).map(Ok).boxed();
		let push_output_stream = src
			.sync(push_arg, push_input_stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the push stream"))?;

		// Start the pull.
		let pull_arg = tg::sync::Arg {
			commands: arg.commands,
			errors: arg.errors,
			eager: arg.eager,
			force: arg.force,
			get: arg.items.clone(),
			local: None,
			logs: arg.logs,
			metadata: arg.metadata,
			outputs: arg.outputs,
			put: Vec::new(),
			recursive: arg.recursive,
			remotes: None,
		};
		let pull_input_stream = ReceiverStream::new(push_output_receiver).map(Ok).boxed();
		let pull_output_stream = dst
			.sync(pull_arg, pull_input_stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the pull stream"))?;

		// Create the push future.
		let push_future = async {
			let mut push_output_stream = pin!(push_output_stream);
			while let Some(message) = push_output_stream.try_next().await? {
				match message {
					tg::sync::Message::Put(tg::sync::PutMessage::Progress(message)) => {
						let processes = message.skipped.processes + message.transferred.processes;
						let objects = message.skipped.objects + message.transferred.objects;
						let bytes = message.skipped.bytes + message.transferred.bytes;
						progress.increment("processes", processes);
						progress.increment("objects", objects);
						progress.increment("bytes", bytes);
						*output.lock().unwrap() += &message;
					},
					tg::sync::Message::End => {
						return Ok(());
					},
					_ => {
						push_output_sender
							.send(message.clone())
							.await
							.map_err(|_| tg::error!("failed to send the message"))?;
					},
				}
			}
			Err(tg::error!("the push did not send the end message"))
		};

		// Create the pull future.
		let pull_future = async {
			let mut pull_output_stream = pin!(pull_output_stream);
			while let Some(message) = pull_output_stream.try_next().await? {
				match message {
					tg::sync::Message::Get(tg::sync::GetMessage::Progress(message)) => {
						let processes = message.skipped.processes + message.transferred.processes;
						let objects = message.skipped.objects + message.transferred.objects;
						let bytes = message.skipped.bytes + message.transferred.bytes;
						progress.increment("processes", processes);
						progress.increment("objects", objects);
						progress.increment("bytes", bytes);
						*output.lock().unwrap() += &message;
					},
					tg::sync::Message::End => {
						return Ok(());
					},
					_ => {
						pull_output_sender
							.send(message.clone())
							.await
							.map_err(|_| tg::error!("failed to send the message"))?;
					},
				}
			}
			Err(tg::error!("the pull did not send the end message"))
		};

		future::try_join(push_future, pull_future).await?;

		progress.finish("processes");
		progress.finish("objects");
		progress.finish("bytes");

		Ok(output.lock().unwrap().clone())
	}

	pub(crate) async fn handle_push_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self
			.push_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the push"))?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
