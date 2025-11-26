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
	tangram_either::Either,
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
		let remote = arg
			.remote
			.as_ref()
			.ok_or_else(|| tg::error!("expected the remote to be set"))?
			.clone();
		let remote = self.get_remote_client(remote).await?;
		Self::push_or_pull(self, &remote, &arg).await
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
		if arg.items.iter().any(Either::is_left) {
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
					Ok(Err(error)) => progress.error(error),
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
							Either::Left(process) => {
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
								let mut complete = true;
								if arg.recursive {
									complete = complete && metadata.children.count.is_some();
									if arg.commands {
										complete = complete
											&& metadata.children_commands.count.is_some()
											&& metadata.children_commands.weight.is_some();
									}
									if arg.outputs {
										complete = complete
											&& metadata.children_outputs.count.is_some()
											&& metadata.children_outputs.weight.is_some();
									}
								} else {
									if arg.commands {
										complete = complete
											&& metadata.command.count.is_some()
											&& metadata.command.weight.is_some();
									}
									if arg.outputs {
										complete = complete
											&& metadata.output.count.is_some()
											&& metadata.output.weight.is_some();
									}
								}
								if complete {
									break Ok::<_, tg::Error>(Either::Left(metadata));
								}
							},
							Either::Right(object) => {
								let metadata_arg = tg::object::metadata::Arg::default();
								let metadata = src
									.try_get_object_metadata(object, metadata_arg)
									.await?
									.ok_or_else(|| tg::error!("expected the metadata to be set"))?;
								if metadata.count.is_some() && metadata.weight.is_some() {
									break Ok::<_, tg::Error>(Either::Right(metadata));
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
				Either::Left(metadata) => {
					if arg.recursive {
						if let Some(children_count) = metadata.children.count {
							*processes.get_or_insert(0) += children_count;
						}
						if arg.commands {
							if let Some(commands_count) = metadata.children_commands.count {
								*objects.get_or_insert(0) += commands_count;
							}
							if let Some(commands_weight) = metadata.children_commands.weight {
								*bytes.get_or_insert(0) += commands_weight;
							}
						}
						if arg.outputs {
							if let Some(outputs_count) = metadata.children_outputs.count {
								*objects.get_or_insert(0) += outputs_count;
							}
							if let Some(outputs_weight) = metadata.children_outputs.weight {
								*bytes.get_or_insert(0) += outputs_weight;
							}
						}
					} else {
						if arg.commands {
							if let Some(command_count) = metadata.command.count {
								*objects.get_or_insert(0) += command_count;
							}
							if let Some(command_weight) = metadata.command.weight {
								*bytes.get_or_insert(0) += command_weight;
							}
						}
						if arg.outputs {
							if let Some(output_count) = metadata.output.count {
								*objects.get_or_insert(0) += output_count;
							}
							if let Some(output_weight) = metadata.output.weight {
								*bytes.get_or_insert(0) += output_weight;
							}
						}
					}
				},
				Either::Right(metadata) => {
					if let Some(count) = metadata.count {
						*objects.get_or_insert(0) += count;
					}
					if let Some(weight) = metadata.weight {
						*bytes.get_or_insert(0) += weight;
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
		let output = Arc::new(Mutex::new(tg::push::Output {
			processes: 0,
			objects: 0,
			bytes: 0,
		}));

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
			eager: arg.eager,
			get: Vec::new(),
			outputs: arg.outputs,
			put: Vec::new(),
			recursive: arg.recursive,
			remote: None,
		};
		let stream = ReceiverStream::new(pull_output_receiver).map(Ok).boxed();
		let push_stream = src
			.sync(push_arg, stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the push stream"))?;

		// Start the pull.
		let pull_arg = tg::sync::Arg {
			commands: arg.commands,
			eager: arg.eager,
			get: arg.items.clone(),
			outputs: arg.outputs,
			put: Vec::new(),
			recursive: arg.recursive,
			remote: None,
		};
		let stream = ReceiverStream::new(push_output_receiver).map(Ok).boxed();
		let pull_stream = dst
			.sync(pull_arg, stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the pull stream"))?;

		// Create the push future.
		let push_future = async {
			let mut push_stream = pin!(push_stream);
			while let Some(message) = push_stream.try_next().await? {
				match message {
					tg::sync::Message::Put(tg::sync::PutMessage::Progress(message)) => {
						progress.increment("processes", message.processes);
						progress.increment("objects", message.objects);
						progress.increment("bytes", message.bytes);
						let mut output = output.lock().unwrap();
						output.processes += message.processes;
						output.objects += message.objects;
						output.bytes += message.bytes;
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
			let mut pull_stream = pin!(pull_stream);
			while let Some(message) = pull_stream.try_next().await? {
				match message {
					tg::sync::Message::Get(tg::sync::GetMessage::Progress(message)) => {
						progress.increment("processes", message.processes);
						progress.increment("objects", message.objects);
						progress.increment("bytes", message.bytes);
						let mut output = output.lock().unwrap();
						output.processes += message.processes;
						output.objects += message.objects;
						output.bytes += message.bytes;
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
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = self.push_with_context(context, arg).await?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
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
