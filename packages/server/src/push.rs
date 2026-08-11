use {
	crate::Session,
	futures::{prelude::*, stream::BoxStream, stream::FuturesUnordered},
	num::ToPrimitive as _,
	std::{
		collections::BTreeSet,
		ops::ControlFlow,
		panic::AssertUnwindSafe,
		pin::pin,
		sync::{Arc, Mutex},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tokio_stream::wrappers::ReceiverStream,
};

struct PushOrPullTaskArg {
	arg: tg::push::Arg,
	destination: tg::Location,
	get: Vec<tg::Referent<tg::Selector<tg::Id>>>,
	process: bool,
	progress: crate::progress::Handle<tg::push::Output>,
	received_specifiers: Option<Arc<Mutex<BTreeSet<tg::Specifier>>>>,
	source: tg::Location,
}

impl Session {
	pub(crate) async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + use<>,
	> {
		let source = arg
			.source
			.clone()
			.unwrap_or_else(|| tg::Location::Local(tg::location::Local::default()));
		let destination = arg.destination.clone().unwrap_or_else(|| {
			tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			})
		});
		let stream = self.push_or_pull(&arg, source, destination).await?;
		Ok(stream)
	}

	pub(crate) async fn push_for_process(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + use<>,
	> {
		let source = arg
			.source
			.clone()
			.unwrap_or_else(|| tg::Location::Local(tg::location::Local::default()));
		let destination = arg.destination.clone().unwrap_or_else(|| {
			tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			})
		});
		let stream = self
			.push_or_pull_for_process(&arg, source, destination)
			.await?;
		Ok(stream)
	}

	pub(crate) async fn push_or_pull(
		&self,
		arg: &tg::push::Arg,
		source: tg::Location,
		destination: tg::Location,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>> {
		let get = arg
			.nodes
			.iter()
			.cloned()
			.map(|node| node.map(tg::Selector::Id))
			.collect();
		self.push_or_pull_inner(arg, false, get, None, source, destination)
			.await
	}

	pub(crate) async fn push_or_pull_with_selectors(
		&self,
		arg: &tg::push::Arg,
		get: Vec<tg::Referent<tg::Selector<tg::Id>>>,
		source: tg::Location,
		destination: tg::Location,
	) -> tg::Result<(
		BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
		Arc<Mutex<BTreeSet<tg::Specifier>>>,
	)> {
		let received_specifiers = Arc::new(Mutex::new(BTreeSet::new()));
		let stream = self
			.push_or_pull_inner(
				arg,
				false,
				get,
				Some(received_specifiers.clone()),
				source,
				destination,
			)
			.await?;
		let output = (stream, received_specifiers);

		Ok(output)
	}

	async fn push_or_pull_for_process(
		&self,
		arg: &tg::push::Arg,
		source: tg::Location,
		destination: tg::Location,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>> {
		let get = arg
			.nodes
			.iter()
			.cloned()
			.map(|node| node.map(tg::Selector::Id))
			.collect();
		self.push_or_pull_inner(arg, true, get, None, source, destination)
			.await
	}

	async fn push_or_pull_inner(
		&self,
		arg: &tg::push::Arg,
		process: bool,
		get: Vec<tg::Referent<tg::Selector<tg::Id>>>,
		received_specifiers: Option<Arc<Mutex<BTreeSet<tg::Specifier>>>>,
		source: tg::Location,
		destination: tg::Location,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>> {
		// Create the progress handle and add the indicators.
		let progress = crate::progress::Handle::new();
		for name in [
			"groups",
			"objects",
			"organizations",
			"processes",
			"sandboxes",
			"tags",
			"users",
		] {
			progress.start(
				name.to_owned(),
				name.to_owned(),
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
			let session = self.clone();
			let source = source.clone();
			let progress = progress.clone();
			let arg = arg.clone();
			|_| async move {
				session
					.push_or_pull_set_indicator_totals(source.clone(), progress, &arg)
					.await
			}
		});

		// Spawn the task.
		let task = Task::spawn({
			let session = self.clone();
			let destination = destination.clone();
			let get = get.clone();
			let progress = progress.clone();
			let received_specifiers = received_specifiers.clone();
			let arg = arg.clone();
			let source = source.clone();
			|_| async move {
				let task_arg = PushOrPullTaskArg {
					arg,
					destination: destination.clone(),
					get,
					process,
					progress: progress.clone(),
					received_specifiers,
					source: source.clone(),
				};
				let result = AssertUnwindSafe(session.push_or_pull_task(task_arg))
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

		Ok(stream.boxed())
	}

	async fn push_or_pull_set_indicator_totals(
		&self,
		source: tg::Location,
		progress: crate::progress::Handle<tg::push::Output>,
		arg: &tg::push::Arg,
	) -> tg::Result<()> {
		let mut metadata_futures = arg
			.nodes
			.iter()
			.filter_map(|node| {
				if node.node.kind() != tg::id::Kind::Process && !node.node.kind().is_object() {
					return None;
				}
				let session = self.clone();
				let source = source.clone();
				Some(async move {
					loop {
						if let Ok(object) = tg::object::Id::try_from(node.node.clone()) {
							let metadata_arg = tg::object::metadata::Arg {
								location: Some(source.clone().into()),
								token: node.options.token.clone(),
							};
							let metadata = session
								.try_get_object_metadata(&object, metadata_arg)
								.await?
								.ok_or_else(|| tg::error!("expected the metadata to be set"))?;
							if metadata.subtree.count.is_some() && metadata.subtree.size.is_some() {
								break Ok::<_, tg::Error>(tg::Either::Left(metadata));
							}
						} else {
							let process = tg::process::Id::try_from(node.node.clone())?;
							let metadata_arg = tg::process::metadata::Arg {
								location: Some(source.clone().into()),
								token: node.options.token.clone(),
							};
							let Some(metadata) = session
								.try_get_process_metadata(&process, metadata_arg)
								.await
								.map_err(|error| tg::error!(!error, "failed to get the process"))?
							else {
								return Err(tg::error!("failed to get the process"));
							};
							let mut stored = true;
							if arg.process_children {
								stored = stored && metadata.subtree.count.is_some();
								if arg.process_commands {
									stored = stored
										&& metadata.subtree.command.count.is_some()
										&& metadata.subtree.command.size.is_some();
								}
								if arg.process_outputs {
									stored = stored
										&& metadata.subtree.output.count.is_some()
										&& metadata.subtree.output.size.is_some();
								}
							} else {
								if arg.process_commands {
									stored = stored
										&& metadata.node.command.count.is_some()
										&& metadata.node.command.size.is_some();
								}
								if arg.process_outputs {
									stored = stored
										&& metadata.node.output.count.is_some()
										&& metadata.node.output.size.is_some();
								}
							}
							if stored {
								break Ok::<_, tg::Error>(tg::Either::Right(metadata));
							}
						}
						tokio::time::sleep(Duration::from_secs(1)).await;
					}
				})
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
					if arg.process_children {
						if let Some(count) = metadata.subtree.count {
							*processes.get_or_insert(0) += count;
						}
						if arg.process_commands {
							if let Some(commands_count) = metadata.subtree.command.count {
								*objects.get_or_insert(0) += commands_count;
							}
							if let Some(commands_size) = metadata.subtree.command.size {
								*bytes.get_or_insert(0) += commands_size;
							}
						}
						if arg.process_outputs {
							if let Some(outputs_count) = metadata.subtree.output.count {
								*objects.get_or_insert(0) += outputs_count;
							}
							if let Some(outputs_size) = metadata.subtree.output.size {
								*bytes.get_or_insert(0) += outputs_size;
							}
						}
					} else {
						if arg.process_commands {
							if let Some(command_count) = metadata.node.command.count {
								*objects.get_or_insert(0) += command_count;
							}
							if let Some(command_size) = metadata.node.command.size {
								*bytes.get_or_insert(0) += command_size;
							}
						}
						if arg.process_outputs {
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

	async fn push_or_pull_task(&self, task_arg: PushOrPullTaskArg) -> tg::Result<tg::push::Output> {
		let PushOrPullTaskArg {
			arg,
			destination,
			get,
			process,
			progress,
			received_specifiers,
			source,
		} = task_arg;
		let retry = &self.server.config.sync.retry;
		let retry = tangram_futures::retry::Options {
			backoff: retry.backoff,
			jitter: retry.jitter,
			max_delay: retry.max_delay,
			max_retries: retry.max_retries,
		};
		let session = self.clone();
		let output = tangram_futures::retry::retry(&retry, || {
			let arg = arg.clone();
			let destination = destination.clone();
			let get = get.clone();
			let progress = progress.clone();
			let received_specifiers = received_specifiers.clone();
			let session = session.clone();
			let source = source.clone();
			async move {
				if let Some(received_specifiers) = &received_specifiers {
					received_specifiers.lock().unwrap().clear();
				}
				let output = Arc::new(Mutex::new(tg::push::Output::default()));

				// Set the progress to zero.
				for name in [
					"groups",
					"objects",
					"organizations",
					"processes",
					"sandboxes",
					"tags",
					"users",
				] {
					progress.set(name, 0);
				}
				progress.set("bytes", 0);

				// Create the channels.
				let (push_output_sender, push_output_receiver) = tokio::sync::mpsc::channel(1024);
				let (pull_output_sender, pull_output_receiver) = tokio::sync::mpsc::channel(1024);

				// Start the push.
				let push_arg = tg::sync::Arg {
					ancestors: arg.ancestors,
					eager: arg.eager,
					get: Vec::new(),
					group_children: arg.group_children,
					location: Some(source.clone().into()),
					metadata: arg.metadata,
					organization_children: arg.organization_children,
					process_children: arg.process_children,
					process_commands: arg.process_commands,
					process_errors: arg.process_errors,
					process_logs: arg.process_logs,
					process_outputs: arg.process_outputs,
					put: Vec::new(),
					sandbox_processes: arg.sandbox_processes,
					tag_targets: arg.tag_targets,
					user_children: arg.user_children,
				};
				let push_input_stream = ReceiverStream::new(pull_output_receiver).map(Ok).boxed();
				let push_output_stream = if process {
					session
						.sync_for_process(push_arg, push_input_stream)
						.await
						.map(futures::StreamExt::boxed)
				} else {
					session
						.sync(push_arg, push_input_stream)
						.await
						.map(futures::StreamExt::boxed)
				}
				.map_err(|error| tg::error!(!error, "failed to create the push stream"))?;

				// Start the pull.
				let pull_arg = tg::sync::Arg {
					ancestors: arg.ancestors,
					eager: arg.eager,
					get: get.clone(),
					group_children: arg.group_children,
					location: Some(destination.clone().into()),
					metadata: arg.metadata,
					organization_children: arg.organization_children,
					process_children: arg.process_children,
					process_commands: arg.process_commands,
					process_errors: arg.process_errors,
					process_logs: arg.process_logs,
					process_outputs: arg.process_outputs,
					put: Vec::new(),
					sandbox_processes: arg.sandbox_processes,
					tag_targets: arg.tag_targets,
					user_children: arg.user_children,
				};
				let pull_input_stream = ReceiverStream::new(push_output_receiver).map(Ok).boxed();
				let pull_output_stream = if process {
					session
						.sync_for_process(pull_arg, pull_input_stream)
						.await
						.map(futures::StreamExt::boxed)
				} else {
					session
						.sync(pull_arg, pull_input_stream)
						.await
						.map(futures::StreamExt::boxed)
				}
				.map_err(|error| tg::error!(!error, "failed to create the pull stream"))?;

				// Create the push future.
				let push_future = async {
					let mut push_output_stream = pin!(push_output_stream);
					while let Some(message) = push_output_stream.try_next().await? {
						match message {
							tg::sync::Message::Put(tg::sync::PutMessage::Progress(message)) => {
								Self::push_or_pull_increment_progress(&progress, &message);
								*output.lock().unwrap() += &message;
							},
							tg::sync::Message::End => {
								return Ok::<_, tg::Error>(true);
							},
							_ => {
								Self::push_or_pull_record_received_specifier(
									&message,
									received_specifiers.as_ref(),
								);
								push_output_sender
									.send(message.clone())
									.await
									.map_err(|_| tg::error!("failed to send the message"))?;
							},
						}
					}
					Ok(false)
				};

				// Create the pull future.
				let pull_future = async {
					let mut pull_output_stream = pin!(pull_output_stream);
					while let Some(message) = pull_output_stream.try_next().await? {
						match message {
							tg::sync::Message::Get(tg::sync::GetMessage::Progress(message)) => {
								Self::push_or_pull_increment_progress(&progress, &message);
								*output.lock().unwrap() += &message;
							},
							tg::sync::Message::End => {
								return Ok::<_, tg::Error>(true);
							},
							_ => {
								pull_output_sender
									.send(message.clone())
									.await
									.map_err(|_| tg::error!("failed to send the message"))?;
							},
						}
					}
					Ok(false)
				};

				let (push_completed, pull_completed) =
					future::try_join(push_future, pull_future).await?;

				if push_completed && pull_completed {
					let mut output = output.lock().unwrap().clone();
					output.nodes = session.create_sync_output_nodes(&arg)?;
					Ok(ControlFlow::Break(output))
				} else {
					Ok(ControlFlow::Continue(tg::error!(
						push_completed = %push_completed,
						pull_completed = %pull_completed,
						"sync ended before receiving all end messages"
					)))
				}
			}
		})
		.await?;

		if let tg::Location::Remote(remote) = &destination {
			self.invalidate_remote_cache(&remote.name).await;
		}
		for name in [
			"groups",
			"objects",
			"organizations",
			"processes",
			"sandboxes",
			"tags",
			"users",
		] {
			progress.finish(name);
		}
		progress.finish("bytes");

		Ok(output)
	}

	fn push_or_pull_record_received_specifier(
		message: &tg::sync::Message,
		received_specifiers: Option<&Arc<Mutex<BTreeSet<tg::Specifier>>>>,
	) {
		let Some(received_specifiers) = received_specifiers else {
			return;
		};
		let tg::sync::Message::Put(tg::sync::PutMessage::Node(node)) = message else {
			return;
		};
		let specifier = match node {
			tg::sync::PutNodeMessage::Group(message) => &message.specifier,
			tg::sync::PutNodeMessage::Object(_)
			| tg::sync::PutNodeMessage::Process(_)
			| tg::sync::PutNodeMessage::Sandbox(_) => return,
			tg::sync::PutNodeMessage::Organization(message) => &message.specifier,
			tg::sync::PutNodeMessage::Tag(message) => &message.specifier,
			tg::sync::PutNodeMessage::User(message) => &message.specifier,
		};
		received_specifiers
			.lock()
			.unwrap()
			.insert(specifier.clone());
	}

	fn push_or_pull_increment_progress(
		progress: &crate::progress::Handle<tg::push::Output>,
		message: &tg::sync::ProgressMessage,
	) {
		let skipped = &message.skipped;
		let transferred = &message.transferred;
		progress.increment("bytes", skipped.bytes + transferred.bytes);
		progress.increment("groups", skipped.groups + transferred.groups);
		progress.increment("objects", skipped.objects + transferred.objects);
		progress.increment(
			"organizations",
			skipped.organizations + transferred.organizations,
		);
		progress.increment("processes", skipped.processes + transferred.processes);
		progress.increment("sandboxes", skipped.sandboxes + transferred.sandboxes);
		progress.increment("tags", skipped.tags + transferred.tags);
		progress.increment("users", skipped.users + transferred.users);
	}

	fn create_sync_output_nodes(
		&self,
		arg: &tg::push::Arg,
	) -> tg::Result<Vec<tg::Referent<tg::Id>>> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		arg.nodes
			.iter()
			.map(|node| {
				let id = node.node.clone();
				let resource = tg::grant::Resource::Id(id.clone());
				let (permissions, expires_at) = if id.kind().is_object() {
					(
						vec![tg::grant::Permission::Object(
							tg::grant::permission::object::Permission::Subtree,
						)],
						now + self
							.server
							.config
							.object
							.grant_time_to_live
							.as_secs()
							.to_i64()
							.unwrap(),
					)
				} else if id.kind() == tg::id::Kind::Process {
					let mut permissions = vec![tg::grant::Permission::Process(
						tg::grant::permission::process::Permission::Subtree,
					)];
					if arg.process_commands {
						permissions.push(tg::grant::Permission::Process(
							tg::grant::permission::process::Permission::SubtreeCommand,
						));
					}
					if arg.process_errors {
						permissions.push(tg::grant::Permission::Process(
							tg::grant::permission::process::Permission::SubtreeError,
						));
					}
					if arg.process_logs {
						permissions.push(tg::grant::Permission::Process(
							tg::grant::permission::process::Permission::SubtreeLog,
						));
					}
					if arg.process_outputs {
						permissions.push(tg::grant::Permission::Process(
							tg::grant::permission::process::Permission::SubtreeOutput,
						));
					}
					let expires_at = now
						+ self
							.server
							.config
							.process
							.grant_time_to_live
							.as_secs()
							.to_i64()
							.unwrap();
					(permissions, expires_at)
				} else if matches!(
					id.kind(),
					tg::id::Kind::Group
						| tg::id::Kind::Organization
						| tg::id::Kind::Sandbox
						| tg::id::Kind::Tag
						| tg::id::Kind::User
				) {
					let token = self.create_read_token(&id)?;
					return Ok(tg::Referent::with_node_and_token(id, token));
				} else {
					return Ok(tg::Referent::with_node(id));
				};
				let token = self.create_token(resource, permissions, expires_at)?;
				let node = tg::Referent::with_node_and_token(id, token);
				Ok(node)
			})
			.collect()
	}

	pub(crate) async fn push_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self
			.push(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the push"))?;

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
				(Some(content_type), BoxBody::with_sse_stream(stream))
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
