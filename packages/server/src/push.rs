use {
	crate::Session,
	futures::{prelude::*, stream::BoxStream, stream::FuturesUnordered},
	num::ToPrimitive as _,
	std::{
		collections::BTreeSet,
		ops::ControlFlow,
		panic::AssertUnwindSafe,
		sync::{Arc, Mutex},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tokio_stream::wrappers::ReceiverStream,
};

type HeaderSender = Arc<Mutex<Option<tokio::sync::oneshot::Sender<tg::Result<tg::push::Header>>>>>;

struct PushOrPullInnerArg<'a> {
	arg: &'a tg::push::Arg,
	destination: tg::Location,
	get: Vec<tg::Referent<tg::Selector<tg::Id>>>,
	process: bool,
	received_specifiers: Option<Arc<Mutex<BTreeSet<tg::Specifier>>>>,
	source: tg::Location,
	sync: Option<tg::Referent<tg::sync::Id>>,
}

struct PushOrPullTaskArg {
	arg: tg::push::Arg,
	destination: tg::Location,
	get: Vec<tg::Referent<tg::Selector<tg::Id>>>,
	header_sender: HeaderSender,
	process: bool,
	progress: crate::progress::Handle<tg::push::Output>,
	received_specifiers: Option<Arc<Mutex<BTreeSet<tg::Specifier>>>>,
	source: tg::Location,
	source_session: Option<tg::Session>,
	sync: Option<tg::Referent<tg::sync::Id>>,
}

impl Session {
	pub(crate) async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<(
		tg::push::Header,
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + use<>,
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
		let (header, stream) = self.push_or_pull(&arg, source, destination).await?;
		Ok((header, stream))
	}

	pub(crate) async fn push_for_process(
		&self,
		arg: tg::push::Arg,
		sync: Option<tg::Referent<tg::sync::Id>>,
	) -> tg::Result<(
		tg::push::Header,
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + use<>,
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
			.push_or_pull_for_process(&arg, source, destination, sync)
			.await?;
		Ok((header, stream))
	}

	pub(crate) async fn push_or_pull(
		&self,
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
		let inner_arg = PushOrPullInnerArg {
			arg,
			destination,
			get,
			process: false,
			received_specifiers: None,
			source,
			sync: None,
		};
		self.push_or_pull_inner(inner_arg).await
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
		let inner_arg = PushOrPullInnerArg {
			arg,
			destination,
			get,
			process: false,
			received_specifiers: Some(received_specifiers.clone()),
			source,
			sync: None,
		};
		let (_, stream) = self.push_or_pull_inner(inner_arg).await?;
		let output = (stream, received_specifiers);

		Ok(output)
	}

	pub(crate) async fn push_or_pull_for_process(
		&self,
		arg: &tg::push::Arg,
		source: tg::Location,
		destination: tg::Location,
		sync: Option<tg::Referent<tg::sync::Id>>,
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
		let inner_arg = PushOrPullInnerArg {
			arg,
			destination,
			get,
			process: true,
			received_specifiers: None,
			source,
			sync,
		};
		self.push_or_pull_inner(inner_arg).await
	}

	async fn push_or_pull_inner(
		&self,
		inner_arg: PushOrPullInnerArg<'_>,
	) -> tg::Result<(
		tg::push::Header,
		BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
	)> {
		let PushOrPullInnerArg {
			arg,
			destination,
			get,
			process,
			received_specifiers,
			source,
			sync,
		} = inner_arg;
		let source_session = match &source {
			tg::Location::Local(_) => None,
			tg::Location::Remote(remote) => {
				let session = if process {
					self.get_remote_session_for_process(&remote.name).await
				} else {
					self.get_remote_session(&remote.name).await
				}
				.map_err(|error| {
					tg::error!(
						!error,
						remote = %remote.name,
						"failed to get the source remote session"
					)
				})?;
				crate::checkpoint!(
					self.server,
					"push.source.remote.resolved",
					remote = %remote.name,
				)
				.await;
				Some(session)
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
					.push_or_pull_set_indicator_totals(source, progress, &arg)
					.await
			}
		});

		// Create the header channel.
		let (header_sender, header_receiver) = tokio::sync::oneshot::channel();
		let header_sender = Arc::new(Mutex::new(Some(header_sender)));

		// Spawn the task.
		let task = Task::spawn({
			let session = self.clone();
			let progress = progress.clone();
			let arg = arg.clone();
			|_| async move {
				let task_arg = PushOrPullTaskArg {
					arg,
					destination,
					get,
					header_sender: header_sender.clone(),
					process,
					progress: progress.clone(),
					received_specifiers,
					source,
					source_session,
					sync,
				};
				let result = AssertUnwindSafe(session.push_or_pull_task(task_arg))
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
		let stream = progress.stream().attach(indicator_total_task).attach(task);

		let header = header_receiver
			.await
			.map_err(|error| tg::error!(!error, "failed to receive the push or pull header"))??;

		Ok((header, stream.boxed()))
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
								tokens: node.options.tokens.clone(),
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

	async fn push_or_pull_task(&self, task_arg: PushOrPullTaskArg) -> tg::Result<tg::push::Output> {
		let PushOrPullTaskArg {
			arg,
			destination,
			get,
			header_sender,
			process,
			progress,
			received_specifiers,
			source,
			source_session,
			sync,
		} = task_arg;
		let trust = source_session.as_ref().is_some_and(tg::Session::trusted);
		let retry = &self.server.config.sync.retry;
		let retry = tangram_futures::retry::Options {
			backoff: retry.backoff,
			jitter: retry.jitter,
			max_delay: retry.max_delay,
			max_retries: retry.max_retries,
		};
		let session = self.clone();
		let sync_state = Arc::new(Mutex::new(sync));
		let output = tangram_futures::retry::retry(&retry, || {
			let arg = arg.clone();
			let destination = destination.clone();
			let get = get.clone();
			let header_sender = header_sender.clone();
			let progress = progress.clone();
			let received_specifiers = received_specifiers.clone();
			let session = session.clone();
			let source = source.clone();
			let source_session = source_session.clone();
			let sync_state = sync_state.clone();
			async move {
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
				let (destination_output_sender, destination_output_receiver) =
					tokio::sync::mpsc::channel(1024);
				let (source_output_sender, source_output_receiver) =
					tokio::sync::mpsc::channel(1024);

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
					sync,
					tag_targets: arg.tag_targets,
					user_children: arg.user_children,
				};
				let destination_input_stream =
					ReceiverStream::new(source_output_receiver).map(Ok).boxed();

				// Create the source future.
				let source_future = async {
					let source_output_stream = if let Some(source_session) = source_session {
						source_session
							.sync(source_arg, source_input_stream)
							.await
							.map(|(_, stream)| stream.boxed())
					} else {
						let arg = crate::sync::InnerArg {
							arg: source_arg,
							process,
							..Default::default()
						};
						session
							.sync_inner(arg, source_input_stream)
							.await
							.map(|(_, stream)| stream.boxed())
					}
					.map_err(|error| tg::error!(!error, "failed to create the source stream"))?;
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
					Ok(completed.then_some(()))
				};

				// Create the destination future.
				let destination_future = async {
					let inner_arg = crate::sync::InnerArg {
						arg: destination_arg,
						process,
						trust,
						..Default::default()
					};
					let (sync_header, destination_output_stream) = session
						.sync_inner(inner_arg, destination_input_stream)
						.await
						.map_err(|error| {
							tg::error!(!error, "failed to create the destination stream")
						})?;

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
					let header = tg::push::Header { nodes };
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
					Ok(completed.then_some((sync_header, get_output)))
				};

				let completed = join(source_future, destination_future).boxed().await?;
				if let Some(((), (sync_header, get_output))) = completed {
					let mut output = output.lock().unwrap().clone();
					output.nodes = session.create_sync_output_nodes(&arg)?;
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
				} else {
					Ok(ControlFlow::Continue(tg::error!(
						"sync ended before receiving all end messages"
					)))
				}
			}
		})
		.boxed()
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
		let now = self.server.clock.unix_timestamp()?;
		arg.nodes
			.iter()
			.map(|node| {
				let id = node.node.clone();
				let (permissions, expires_at) = if id.kind().is_object() {
					(
						vec![tg::authorization::Permission::Object(
							tg::authorization::permission::object::Permission::Subtree,
						)],
						now + self
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
						+ self
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
					let token = self.create_read_token(&id)?;
					return Ok(tg::Referent::with_node_and_local_tokens(id, token));
				} else {
					return Ok(tg::Referent::with_node(id));
				};
				let token = self.create_token(id.clone(), permissions, expires_at)?;
				let node = tg::Referent::with_node_and_local_tokens(id, token);
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
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Get the header and stream.
		let (header, stream) = self
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
	left: impl Future<Output = tg::Result<Option<L>>> + Send,
	right: impl Future<Output = tg::Result<Option<R>>> + Send,
) -> tg::Result<Option<(L, R)>> {
	match future::try_select(left.boxed(), right.boxed()).await {
		Ok(future::Either::Left((Some(left), right))) => {
			Ok(right.await?.map(|right| (left, right)))
		},
		Ok(future::Either::Right((Some(right), left))) => Ok(left.await?.map(|left| (left, right))),
		Ok(future::Either::Left((None, _)) | future::Either::Right((None, _))) => Ok(None),
		Err(future::Either::Left((error, _)) | future::Either::Right((error, _))) => Err(error),
	}
}
