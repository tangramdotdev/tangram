use {
	futures::{
		FutureExt as _, future::BoxFuture, prelude::*, stream::BoxStream, stream::FuturesUnordered,
	},
	num::ToPrimitive as _,
	std::{
		collections::BTreeSet,
		ops::ControlFlow,
		panic::AssertUnwindSafe,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::{Session, push::PushOrPullForProcessArg},
	tokio_stream::wrappers::ReceiverStream,
};

type HeaderSender = Arc<Mutex<Option<tokio::sync::oneshot::Sender<tg::Result<tg::push::Header>>>>>;

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

struct PushOrPullInnerArg<'a> {
	arg: &'a tg::push::Arg,
	destination: tg::Location,
	get: Vec<tg::Referent<tg::Selector<tg::Id>>>,
	join: Option<tg::sync::Join>,
	process: bool,
	received_specifiers: Option<Arc<Mutex<BTreeSet<tg::Specifier>>>>,
	retry: tangram_futures::retry::Options,
	source: tg::Location,
	sync: Option<tg::Referent<tg::sync::Id>>,
}

struct PushOrPullTaskArg {
	arg: tg::push::Arg,
	destination: tg::Location,
	get: Vec<tg::Referent<tg::Selector<tg::Id>>>,
	header_sender: HeaderSender,
	join: Option<tg::sync::Join>,
	process: bool,
	progress: tg::progress::Handle<tg::push::Output>,
	received_specifiers: Option<Arc<Mutex<BTreeSet<tg::Specifier>>>>,
	retry: tangram_futures::retry::Options,
	source: tg::Location,
	source_session: Option<tg::Session>,
	sync: Option<tg::Referent<tg::sync::Id>>,
}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn push(
		&self,
		session: &Session,
		arg: tg::push::Arg,
	) -> tg::Result<(
		tg::push::Header,
		BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
	)> {
		let source = arg.source.clone().unwrap_or_else(|| {
			tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			})
		});
		let destination = arg.destination.clone().unwrap_or_else(|| {
			tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			})
		});
		let (header, stream) = self
			.push_or_pull(session, &arg, source, destination)
			.await?;
		Ok((header, stream))
	}

	/// Push a process's objects, reusing the given sync's id at the destination when there is one, and retrying with the given options.
	pub(crate) async fn push_for_process(
		&self,
		session: &Session,
		arg: tg::push::Arg,
		sync: Option<tg::Referent<tg::sync::Id>>,
		join: Option<tg::sync::Join>,
		retry: tangram_futures::retry::Options,
	) -> tg::Result<(
		tg::push::Header,
		BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
	)> {
		let source = arg
			.source
			.clone()
			.unwrap_or_else(|| tg::Location::Local(tg::location::Local::default()));
		let destination = arg
			.destination
			.clone()
			.ok_or_else(|| tg::error!("expected a destination"))?;
		let process_arg = PushOrPullForProcessArg {
			destination,
			join,
			push_arg: &arg,
			retry,
			source,
			sync,
		};
		let (header, stream) = self.push_or_pull_for_process(session, process_arg).await?;
		Ok((header, stream))
	}

	pub async fn push_or_pull(
		&self,
		session: &Session,
		arg: &tg::push::Arg,
		source: tg::Location,
		destination: tg::Location,
	) -> tg::Result<(
		tg::push::Header,
		BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
	)> {
		let get = arg
			.nodes
			.iter()
			.cloned()
			.map(|node| node.map(tg::Selector::Id))
			.collect();
		let retry = session.server.config.sync.retry.clone().into();
		let inner_arg = PushOrPullInnerArg {
			arg,
			destination,
			get,
			join: None,
			process: false,
			received_specifiers: None,
			retry,
			source,
			sync: None,
		};
		self.push_or_pull_inner(session, inner_arg).await
	}

	pub(crate) async fn push_or_pull_with_selectors(
		&self,
		session: &Session,
		arg: &tg::push::Arg,
		get: Vec<tg::Referent<tg::Selector<tg::Id>>>,
		source: tg::Location,
		destination: tg::Location,
	) -> tg::Result<(
		BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
		Arc<Mutex<BTreeSet<tg::Specifier>>>,
	)> {
		let received_specifiers = Arc::new(Mutex::new(BTreeSet::new()));
		let retry = session.server.config.sync.retry.clone().into();
		let inner_arg = PushOrPullInnerArg {
			arg,
			destination,
			get,
			join: None,
			process: false,
			received_specifiers: Some(received_specifiers.clone()),
			retry,
			source,
			sync: None,
		};
		let (_, stream) = self.push_or_pull_inner(session, inner_arg).await?;
		let output = (stream, received_specifiers);

		Ok(output)
	}

	pub async fn push_or_pull_for_process(
		&self,
		session: &Session,
		arg: PushOrPullForProcessArg<'_>,
	) -> tg::Result<(
		tg::push::Header,
		BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
	)> {
		let PushOrPullForProcessArg {
			destination,
			join,
			push_arg,
			retry,
			source,
			sync,
		} = arg;
		let get = push_arg
			.nodes
			.iter()
			.cloned()
			.map(|node| node.map(tg::Selector::Id))
			.collect();
		let inner_arg = PushOrPullInnerArg {
			arg: push_arg,
			destination,
			get,
			join,
			process: true,
			received_specifiers: None,
			retry,
			source,
			sync,
		};
		self.push_or_pull_inner(session, inner_arg).await
	}

	async fn push_or_pull_inner(
		&self,
		session: &Session,
		inner_arg: PushOrPullInnerArg<'_>,
	) -> tg::Result<(
		tg::push::Header,
		BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
	)> {
		let PushOrPullInnerArg {
			arg,
			destination,
			get,
			join,
			process,
			received_specifiers,
			retry,
			source,
			sync,
		} = inner_arg;
		let source_session = match &source {
			tg::Location::Local(_) => None,
			tg::Location::Remote(remote) => {
				let client = if process {
					session
						.server
						.components
						.remote
						.get_remote_session_for_process(session, &remote.name)
						.await
				} else {
					session
						.server
						.components
						.remote
						.get_remote_session(session, &remote.name)
						.await
				}
				.map_err(|error| {
					tg::error!(
						!error,
						remote = %remote.name,
						"failed to get the source remote session"
					)
				})?;
				tangram_server_core::checkpoint!(
					session.server,
					"push.source.remote.resolved",
					remote = %remote.name,
				)
				.await;
				Some(client)
			},
		};

		// Preserve the destination's local tokens alongside the source tokens for the sync protocol.
		let get = get
			.into_iter()
			.map(|mut node| {
				let original_tokens = std::mem::take(&mut node.options.tokens);
				node.options.tokens = if destination.is_remote() {
					let mut tokens = original_tokens.for_location(&destination);
					if let Some(entry) = original_tokens.get(&source) {
						tokens.set(destination.clone(), entry.clone());
					}
					tokens
				} else {
					let mut tokens =
						tg::authorization::Tokens::with_local(original_tokens.local().cloned());
					if let Some(entry) = original_tokens.get(&source) {
						tokens.set(source.clone(), entry.clone());
					}
					tokens
				};
				node
			})
			.collect::<Vec<_>>();
		// Create the progress handle and add the indicators.
		let capacity = session.server.config.progress.channel_capacity;
		let progress = tg::progress::Handle::new(capacity);
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
			let component = self.clone();
			let session = session.clone();
			let source = source.clone();
			let progress = progress.clone();
			let arg = arg.clone();
			|_| async move {
				component
					.push_or_pull_set_indicator_totals(&session, source, progress, &arg)
					.await
			}
		});

		// Create the header channel.
		let (header_sender, header_receiver) = tokio::sync::oneshot::channel();
		let header_sender = Arc::new(Mutex::new(Some(header_sender)));

		// Spawn the task.
		let task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
			let progress = progress.clone();
			let arg = arg.clone();
			|_| async move {
				let task_arg = PushOrPullTaskArg {
					arg,
					destination,
					get,
					header_sender: header_sender.clone(),
					join,
					process,
					progress: progress.clone(),
					received_specifiers,
					retry,
					source,
					source_session,
					sync,
				};
				let result = AssertUnwindSafe(component.push_or_pull_task(&session, task_arg))
					.catch_unwind()
					.boxed()
					.await;
				match result {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						if let Some(sender) = header_sender.lock().unwrap().take() {
							sender.send(Err(error.clone())).ok();
						}
						progress.error(error);
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						let error = tg::error!(?message, "the task panicked");
						if let Some(sender) = header_sender.lock().unwrap().take() {
							sender.send(Err(error.clone())).ok();
						}
						progress.error(error);
					},
				}
			}
		});

		// Create the stream.
		let stream = progress
			.stream(session.server.config.progress.interval)
			.attach(indicator_total_task)
			.attach(task);

		let header = header_receiver
			.await
			.map_err(|error| tg::error!(!error, "failed to receive the push or pull header"))??;

		Ok((header, stream.boxed()))
	}

	async fn push_or_pull_set_indicator_totals(
		&self,
		session: &Session,
		source: tg::Location,
		progress: tg::progress::Handle<tg::push::Output>,
		arg: &tg::push::Arg,
	) -> tg::Result<()> {
		let poll_interval = session.server.config.sync.metadata_poll_interval;
		let mut metadata_futures = arg
			.nodes
			.iter()
			.filter_map(|node| {
				if node.node.kind() != tg::id::Kind::Process && !node.node.kind().is_object() {
					return None;
				}
				let session = session.clone();
				let source = source.clone();
				Some(async move {
					loop {
						if let Ok(object) = tg::object::Id::try_from(node.node.clone()) {
							let metadata_arg = tg::object::metadata::Arg {
								location: Some(source.clone().into()),
								tokens: node.options.tokens.clone(),
							};
							let metadata = session
								.server
								.components
								.object
								.try_get_object_metadata(&session, &object, metadata_arg)
								.await?
								.ok_or_else(|| tg::error!("expected the metadata to be set"))?;
							if metadata.subtree.count.is_some() && metadata.subtree.size.is_some() {
								break Ok::<_, tg::Error>(tg::Either::Left(metadata));
							}
						} else {
							let process = tg::process::Id::try_from(node.node.clone())?;
							let metadata_arg = tg::process::metadata::Arg {
								location: Some(source.clone().into()),
								tokens: node.options.tokens.clone(),
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
								if arg.process_command_objects {
									stored = stored
										&& metadata.subtree.command_objects.count.is_some()
										&& metadata.subtree.command_objects.size.is_some();
								}
								if arg.process_output_objects {
									stored = stored
										&& metadata.subtree.output_objects.count.is_some()
										&& metadata.subtree.output_objects.size.is_some();
								}
							} else {
								if arg.process_command_objects {
									stored = stored
										&& metadata.node.command_objects.count.is_some()
										&& metadata.node.command_objects.size.is_some();
								}
								if arg.process_output_objects {
									stored = stored
										&& metadata.node.output_objects.count.is_some()
										&& metadata.node.output_objects.size.is_some();
								}
							}
							if stored {
								break Ok::<_, tg::Error>(tg::Either::Right(metadata));
							}
						}
						tokio::time::sleep(poll_interval).await;
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
						if arg.process_command_objects {
							if let Some(commands_count) = metadata.subtree.command_objects.count {
								*objects.get_or_insert(0) += commands_count;
							}
							if let Some(commands_size) = metadata.subtree.command_objects.size {
								*bytes.get_or_insert(0) += commands_size;
							}
						}
						if arg.process_output_objects {
							if let Some(outputs_count) = metadata.subtree.output_objects.count {
								*objects.get_or_insert(0) += outputs_count;
							}
							if let Some(outputs_size) = metadata.subtree.output_objects.size {
								*bytes.get_or_insert(0) += outputs_size;
							}
						}
					} else {
						if arg.process_command_objects {
							if let Some(command_count) = metadata.node.command_objects.count {
								*objects.get_or_insert(0) += command_count;
							}
							if let Some(command_size) = metadata.node.command_objects.size {
								*bytes.get_or_insert(0) += command_size;
							}
						}
						if arg.process_output_objects {
							if let Some(output_count) = metadata.node.output_objects.count {
								*objects.get_or_insert(0) += output_count;
							}
							if let Some(output_size) = metadata.node.output_objects.size {
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

	async fn push_or_pull_task(
		&self,
		session: &Session,
		task_arg: PushOrPullTaskArg,
	) -> tg::Result<tg::push::Output> {
		let PushOrPullTaskArg {
			arg,
			destination,
			get,
			header_sender,
			join,
			process,
			progress,
			received_specifiers,
			retry,
			source,
			source_session,
			sync,
		} = task_arg;
		let trust = source_session.as_ref().is_some_and(tg::Session::trusted);
		let session = session.clone();
		let sync_state = Arc::new(Mutex::new(sync));
		let output = tangram_futures::retry::retry(&retry, || {
			let arg = arg.clone();
			let destination = destination.clone();
			let get = get.clone();
			let header_sender = header_sender.clone();
			let join = join.clone();
			let progress = progress.clone();
			let received_specifiers = received_specifiers.clone();
			let session = session.clone();
			let source = source.clone();
			let source_session = source_session.clone();
			let sync_state = sync_state.clone();
			let server = session.server.clone();
			async move {
				let result = async move {
					let sync = sync_state.lock().unwrap().clone();
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
					let capacity = session.server.config.sync.push_channel_capacity;
					let (destination_output_sender, destination_output_receiver) =
						tokio::sync::mpsc::channel(capacity);
					let (source_output_sender, source_output_receiver) =
						tokio::sync::mpsc::channel(capacity);

					// Create the source arg and input stream.
					let source_location = match &source {
						tg::Location::Local(local) => tg::Location::Local(local.clone()),
						tg::Location::Remote(remote) => tg::Location::Local(tg::location::Local {
							region: remote.region.clone(),
						}),
					};
					let source_arg = tg::sync::Arg {
						ancestors: arg.ancestors,
						eager: arg.eager,
						force: arg.force,
						get: Vec::new(),
						group_children: arg.group_children,
						location: Some(source_location.into()),
						metadata: arg.metadata,
						organization_children: arg.organization_children,
						process_children: arg.process_children,
						process_command_objects: arg.process_command_objects,
						process_error_objects: arg.process_error_objects,
						process_log_objects: arg.process_log_objects,
						process_output_objects: arg.process_output_objects,
						put: Vec::new(),
						sandbox_processes: arg.sandbox_processes,
						sync: None,
						tag_targets: arg.tag_targets,
						user_children: arg.user_children,
					};
					let source_input_stream = ReceiverStream::new(destination_output_receiver)
						.map(Ok)
						.boxed();

					// Create the destination arg and input stream.
					let get_empty = get.is_empty();
					let destination_arg = tg::sync::Arg {
						ancestors: arg.ancestors,
						eager: arg.eager,
						force: arg.force,
						get,
						group_children: arg.group_children,
						location: Some(destination.clone().into()),
						metadata: arg.metadata,
						organization_children: arg.organization_children,
						process_children: arg.process_children,
						process_command_objects: arg.process_command_objects,
						process_error_objects: arg.process_error_objects,
						process_log_objects: arg.process_log_objects,
						process_output_objects: arg.process_output_objects,
						put: Vec::new(),
						sandbox_processes: arg.sandbox_processes,
						sync: sync.map(tg::sync::Join::Sync).or_else(|| join.clone()),
						tag_targets: arg.tag_targets,
						user_children: arg.user_children,
					};
					let destination_input_stream =
						ReceiverStream::new(source_output_receiver).map(Ok).boxed();

					// Create the source future.
					let source_future = async {
						let source_output = if let Some(source_session) = source_session {
							source_session
								.sync(source_arg, source_input_stream)
								.await
								.map(|output| output.map_break(|(_, stream)| stream.boxed()))
						} else {
							let arg = tangram_server_core::sync::InnerArg {
								arg: source_arg,
								process,
								..Default::default()
							};
							session
								.server
								.components
								.sync
								.sync_inner(&session, arg, source_input_stream)
								.await
								.map(|output| output.map_break(|(_, stream)| stream.boxed()))
						}
						.map_err(|error| {
							tg::error!(!error, "failed to create the source stream")
						})?;

						// End the attempt if the source could not be reached.
						let source_output_stream = match source_output {
							ControlFlow::Break(stream) => stream,
							ControlFlow::Continue(error) => {
								let error =
									tg::error!(!error, "failed to create the source stream");
								return Ok(ControlFlow::Continue(error));
							},
						};
						let completed = forward(
							source_output_stream.boxed(),
							&source_output_sender,
							|message| match message {
								tg::sync::Message::Put(tg::sync::PutMessage::Progress(message)) => {
									Self::push_or_pull_increment_progress(&progress, &message);
									*output.lock().unwrap() += &message;
									None
								},
								message => {
									Self::push_or_pull_record_received_specifier(
										&message,
										received_specifiers.as_ref(),
									);
									Some(message)
								},
							},
						)
						.await?;
						Ok(Self::push_or_pull_completion(completed, ()))
					};

					// Create the destination future.
					let destination_future = async {
						let inner_arg = tangram_server_core::sync::InnerArg {
							arg: destination_arg,
							process,
							trust,
							..Default::default()
						};
						let destination_output = session
							.server
							.components
							.sync
							.sync_inner(&session, inner_arg, destination_input_stream)
							.await
							.map_err(|error| {
								tg::error!(!error, "failed to create the destination stream")
							})?;

						// End the attempt if the destination could not be reached.
						let (sync_header, destination_output_stream) = match destination_output {
							ControlFlow::Break(output) => output,
							ControlFlow::Continue(error) => {
								let error =
									tg::error!(!error, "failed to create the destination stream");
								return Ok(ControlFlow::Continue(error));
							},
						};

						// Preserve the sync so the header remains valid across transfer retries.
						*sync_state.lock().unwrap() = sync_header.sync.clone();

						// Send the header before transferring the nodes.
						let mut nodes = arg.nodes.clone();
						if let Some(sync) = &sync_header.sync {
							let mut tokens = tg::authorization::Tokens::default();
							for token in sync.options.tokens.local_authorization() {
								tokens.insert_authorization(destination.clone(), token.clone());
							}
							for node in &mut nodes {
								node.options.tokens.inherit(&tokens);
							}
						}
						let sync = sync_header.sync.as_ref().map(|sync| {
							let mut tokens = tg::authorization::Tokens::default();
							for token in sync.options.tokens.local_authorization() {
								tokens.insert_authorization(destination.clone(), token.clone());
							}
							let options = tg::referent::Options {
								tokens,
								..Default::default()
							};
							tg::Referent::new(sync.node.clone(), options)
						});
						let header = tg::push::Header { nodes, sync };
						if let Some(sender) = header_sender.lock().unwrap().take() {
							sender.send(Ok(header)).ok();
						}

						let mut get_output = None;
						let completed = forward(
							destination_output_stream.boxed(),
							&destination_output_sender,
							|message| match message {
								tg::sync::Message::Get(tg::sync::GetMessage::Output(message)) => {
									get_output = Some(message);
									None
								},
								tg::sync::Message::Get(tg::sync::GetMessage::Progress(message)) => {
									Self::push_or_pull_increment_progress(&progress, &message);
									*output.lock().unwrap() += &message;
									None
								},
								message => Some(message),
							},
						)
						.await?;
						Ok(Self::push_or_pull_completion(
							completed,
							(sync_header, get_output),
						))
					};

					let completed = self::join(source_future, destination_future)
						.boxed()
						.await?;
					let ((), (sync_header, get_output)) = match completed {
						ControlFlow::Break(output) => output,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					let mut output = output.lock().unwrap().clone();
					output.nodes = Self::create_sync_output_nodes(&session, &arg)?;
					if let Some(sync) = sync_header.sync {
						let mut authorization_tokens = tg::authorization::Tokens::default();
						for token in sync.options.tokens.local_authorization() {
							authorization_tokens
								.insert_authorization(destination.clone(), token.clone());
						}
						for node in &mut output.nodes {
							node.options.tokens.inherit(&authorization_tokens);
						}
					}

					// Add the destination's authorization tokens from the get output to the push output nodes.
					if !get_empty && get_output.is_none() {
						return Err(tg::error!("the destination did not send the get output"));
					}
					let get_output_nodes = get_output.into_iter().flat_map(|output| output.nodes);
					for get_output_node in get_output_nodes {
						let mut tokens = tg::authorization::Tokens::default();
						for token in get_output_node.options.tokens.local_authorization() {
							tokens.insert_authorization(destination.clone(), token.clone());
						}
						for node in &mut output.nodes {
							if node.node == get_output_node.node {
								node.options.tokens.inherit(&tokens);
							}
						}
					}

					Ok(ControlFlow::Break(output))
				}
				.await;

				// Retry the attempt if it ended before the sync completed.
				if let Ok(ControlFlow::Continue(error)) = &result {
					tracing::warn!(error = %error.trace(), "retrying the push or pull");
					tangram_server_core::checkpoint!(server, "push.attempt.failed").await;
				}
				result
			}
		})
		.boxed()
		.await?;

		if let tg::Location::Remote(remote) = &destination {
			session
				.server
				.components
				.remote
				.invalidate_remote_cache(&session, &remote.name)
				.await;
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

	/// Get the output of a side of a push or pull attempt, continuing if its stream ended before its end messages.
	fn push_or_pull_completion<T>(completed: bool, output: T) -> ControlFlow<T, tg::Error> {
		if completed {
			ControlFlow::Break(output)
		} else {
			let error = tg::error!("sync ended before receiving all end messages");
			ControlFlow::Continue(error)
		}
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
		progress: &tg::progress::Handle<tg::push::Output>,
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
		session: &Session,
		arg: &tg::push::Arg,
	) -> tg::Result<Vec<tg::Referent<tg::Id>>> {
		let now = session.server.clock.unix_timestamp();
		arg.nodes
			.iter()
			.map(|node| {
				let id = node.node.clone();
				let (permissions, expires_at) = if id.kind().is_object() {
					(
						vec![tg::authorization::Permission::Object(
							tg::authorization::permission::object::Permission::Subtree,
						)],
						now + session
							.server
							.config
							.object
							.permission_time_to_live
							.as_secs()
							.to_i64()
							.unwrap(),
					)
				} else if id.kind() == tg::id::Kind::Process {
					let mut permissions = vec![tg::authorization::Permission::Process(
						tg::authorization::permission::process::Permission::Subtree,
					)];
					if arg.process_command_objects {
						permissions.push(tg::authorization::Permission::Process(
							tg::authorization::permission::process::Permission::SubtreeCommandObjects,
						));
					}
					if arg.process_error_objects {
						permissions.push(tg::authorization::Permission::Process(
							tg::authorization::permission::process::Permission::SubtreeErrorObjects,
						));
					}
					if arg.process_log_objects {
						permissions.push(tg::authorization::Permission::Process(
							tg::authorization::permission::process::Permission::SubtreeLogObjects,
						));
					}
					if arg.process_output_objects {
						permissions.push(tg::authorization::Permission::Process(
							tg::authorization::permission::process::Permission::SubtreeOutputObjects,
						));
					}
					let expires_at = now
						+ session
							.server
							.config
							.process
							.permission_time_to_live
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
					let token = session
						.server
						.components
						.authorization
						.create_read_token(session, &id)?;
					return Ok(tg::Referent::with_node_and_local_tokens(id, token));
				} else {
					return Ok(tg::Referent::with_node(id));
				};
				let token = session.server.components.authorization.create_token(
					session,
					id.clone(),
					permissions,
					expires_at,
				)?;
				let node = tg::Referent::with_node_and_local_tokens(id, token);
				Ok(node)
			})
			.collect()
	}

	pub(crate) async fn push_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Get the header and stream.
		let (header, stream) = self
			.push(session, arg)
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};

		let body = tangram_http::body::header::set(
			body,
			&header,
			tangram_http::body::encoding::Encoding::Json,
		)
		.map_err(|error| tg::error!(!error, "failed to serialize the header"))?;

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["push"]) => self.push_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::push::Component for Component {
	fn push<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::push::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<(
			tg::push::Header,
			BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
		)>,
	> {
		self.push(session, arg).boxed()
	}

	fn push_for_process<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::push::Arg,
		sync: Option<tg::Referent<tg::sync::Id>>,
		join: Option<tg::sync::Join>,
		retry: tangram_futures::retry::Options,
	) -> BoxFuture<
		'a,
		tg::Result<(
			tg::push::Header,
			BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
		)>,
	> {
		self.push_for_process(session, arg, sync, join, retry)
			.boxed()
	}

	fn push_or_pull<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: &'a tg::push::Arg,
		source: tg::Location,
		destination: tg::Location,
	) -> BoxFuture<
		'a,
		tg::Result<(
			tg::push::Header,
			BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
		)>,
	> {
		self.push_or_pull(session, arg, source, destination).boxed()
	}

	fn push_or_pull_for_process<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: PushOrPullForProcessArg<'a>,
	) -> BoxFuture<
		'a,
		tg::Result<(
			tg::push::Header,
			BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
		)>,
	> {
		self.push_or_pull_for_process(session, arg).boxed()
	}

	fn push_or_pull_with_selectors<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: &'a tg::push::Arg,
		get: Vec<tg::Referent<tg::Selector<tg::Id>>>,
		source: tg::Location,
		destination: tg::Location,
	) -> BoxFuture<
		'a,
		tg::Result<(
			BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
			Arc<Mutex<BTreeSet<tg::Specifier>>>,
		)>,
	> {
		self.push_or_pull_with_selectors(session, arg, get, source, destination)
			.boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}

async fn forward(
	mut stream: BoxStream<'_, tg::Result<tg::sync::Message>>,
	sender: &tokio::sync::mpsc::Sender<tg::sync::Message>,
	mut handle: impl FnMut(tg::sync::Message) -> Option<tg::sync::Message>,
) -> tg::Result<bool> {
	let mut completed = false;
	while let Some(message) = stream.try_next().await? {
		if completed {
			return Err(tg::error!("received a sync message after the end message"));
		}
		if matches!(message, tg::sync::Message::End) {
			completed = true;
			continue;
		}
		if let Some(message) = handle(message) {
			sender.send(message).await.ok();
		}
	}
	Ok(completed)
}

async fn join<L, R>(
	left: impl Future<Output = tg::Result<ControlFlow<L, tg::Error>>> + Send,
	right: impl Future<Output = tg::Result<ControlFlow<R, tg::Error>>> + Send,
) -> tg::Result<ControlFlow<(L, R), tg::Error>> {
	match future::try_select(left.boxed(), right.boxed()).await {
		Ok(future::Either::Left((ControlFlow::Break(left), right))) => {
			Ok(right.await?.map_break(|right| (left, right)))
		},
		Ok(future::Either::Right((ControlFlow::Break(right), left))) => {
			Ok(left.await?.map_break(|left| (left, right)))
		},
		Ok(
			future::Either::Left((ControlFlow::Continue(error), _))
			| future::Either::Right((ControlFlow::Continue(error), _)),
		) => Ok(ControlFlow::Continue(error)),
		Err(future::Either::Left((error, _)) | future::Either::Right((error, _))) => Err(error),
	}
}
