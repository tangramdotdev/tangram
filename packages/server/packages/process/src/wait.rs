use {
	crate::Component,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::BoxFuture,
		stream::{self, BoxStream, FuturesUnordered},
	},
	std::sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	tangram_client::prelude::*,
	tangram_futures::{future::Ext as _, task::Stopper},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
};

impl Component {
	pub async fn try_wait_process_future(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::wait::Arg,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>>> {
		let output = self
			.try_wait_process_future_inner(session, process, arg)
			.await?;

		// Let tests observe that the request reached the server before it changes what the request observes.
		if output.is_some() {
			let process = match process {
				tg::Either::Left(nonce) => nonce.to_string(),
				tg::Either::Right(id) => id.to_string(),
			};
			tangram_server_core::checkpoint!(session.server, "process.wait.subscribed", process = %process).await;
		}

		Ok(output)
	}

	async fn try_wait_process_future_inner(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		mut arg: tg::process::wait::Arg,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>>> {
		let mut process = process.clone();
		if self
			.try_resolve_process(
				session,
				&mut process,
				&mut arg.location,
				Some(&mut arg.tokens),
				&[],
			)
			.await?
			.is_none()
		{
			return Ok(None);
		}
		let tg::Either::Right(id) = &process else {
			return self.try_wait_process_forward(session, &process, arg).await;
		};

		// A leased wait must survive graceful shutdown until completion or client disconnect.
		let mut session = session.clone();
		if arg.lease.is_some() {
			session.context.stopper = None;
		}
		self.try_wait_process_future_with_cancel(&session, id, arg, Arc::new(AtomicBool::new(true)))
			.await
	}

	pub(crate) async fn create_wait_process_finished_future_local(
		&self,
		session: &Session,
		id: &tg::process::Id,
	) -> tg::Result<BoxFuture<'static, tg::Result<()>>> {
		if let Some(mut runner) = Self::try_get_process_runner_inner(session, id, None) {
			let component = self.clone();
			let session = session.clone();
			let id = id.clone();
			let future = async move {
				loop {
					let status = runner.processes.get(&id).map(|process| process.data.status);
					let Some(status) = status else {
						break;
					};
					if status.is_finished() {
						return Ok(());
					}
					if runner.changed.changed().await.is_err() {
						break;
					}
				}

				let wakeups = component
					.create_process_status_wakeup_stream(&session, &id, None, None)
					.await?;
				let stream = component.create_process_data_stream_local(
					&session,
					&id,
					None,
					Some(wakeups),
					tg::process::Source::Auto,
				);
				Self::wait_process_finished_stream(stream).await?;

				Ok(())
			}
			.boxed();
			return Ok(future);
		}

		let wakeups = self
			.create_process_status_wakeup_stream(session, id, None, None)
			.await?;
		let stream = self.create_process_data_stream_local(
			session,
			id,
			None,
			Some(wakeups),
			tg::process::Source::Auto,
		);
		let future = Self::wait_process_finished_stream(stream).boxed();

		Ok(future)
	}

	async fn wait_process_finished_stream(
		mut stream: BoxStream<'static, tg::Result<tg::process::Data>>,
	) -> tg::Result<()> {
		while let Some(data) = stream.try_next().await? {
			if data.status.is_finished() {
				return Ok(());
			}
		}
		Err(tg::error!(
			"the process status stream ended before the process finished"
		))
	}

	async fn try_wait_process_stream(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::wait::Arg,
	) -> tg::Result<
		Option<impl futures::Stream<Item = tg::Result<tg::process::wait::Event>> + Send + use<>>,
	> {
		let Some(future) = self.try_wait_process_future(session, process, arg).await? else {
			return Ok(None);
		};
		let stream = stream::once(future).filter_map(|result| async move {
			match result {
				Ok(Some(outcome)) => Some(Ok(tg::process::wait::Event::Outcome(outcome))),
				Ok(None) => None,
				Err(error) => Some(Err(error)),
			}
		});
		Ok(Some(stream))
	}

	pub(super) async fn try_wait_process_future_with_cancel(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
		cancel: Arc<AtomicBool>,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>>> {
		// This session owns cancellation; downstream waits only observe the process.
		let mut observe_arg = arg.clone();
		observe_arg.lease = None;
		let attach = |future, location: tg::Location| {
			Self::attach_wait_process_guard(
				session,
				id,
				&arg,
				Some(location.into()),
				cancel,
				future,
			)
		};

		let arg = &observe_arg;
		let process = tg::Either::Right(id.clone());
		if !arg.source.is_index()
			&& let Some((future, location)) = self.try_wait_process_runner(session, id, arg).await?
		{
			return Ok(Some(attach(future, location)));
		}

		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		if let Some(local) = &locations.local {
			if local.current
				&& let Some(future) = self
					.try_wait_process_local(
						session,
						id,
						arg.tokens.local_authorization().to_vec(),
						arg.source,
					)
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?
			{
				let location = tg::Location::Local(tg::location::Local::default());
				return Ok(Some(attach(future, location)));
			}

			if let Some((future, region)) = self
				.try_wait_process_regions(
					session,
					&process,
					arg.lease,
					arg.tokens.clone(),
					&local.regions,
					arg.source,
				)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to wait for the process in another region"),
				)? {
				let location = tg::Location::Local(tg::location::Local {
					region: Some(region),
				});
				return Ok(Some(attach(future, location)));
			}
		}

		let Some((future, remote)) = self
			.try_wait_process_remotes(
				session,
				&process,
				arg.lease,
				arg.tokens.clone(),
				&locations.remotes,
				arg.source,
			)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to wait for the process on the remote"),
			)?
		else {
			return Ok(None);
		};
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});

		Ok(Some(attach(future, location)))
	}

	async fn try_wait_process_forward(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::wait::Arg,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>>> {
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		if let Some(local) = &locations.local
			&& let Some((future, _)) = self
				.try_wait_process_regions(
					session,
					process,
					arg.lease,
					arg.tokens.clone(),
					&local.regions,
					arg.source,
				)
				.await
				.map_err(
					|error| tg::error!(!error, %process, "failed to wait for the process in another region"),
				)? {
			return Ok(Some(future));
		}
		let Some((future, _)) = self
			.try_wait_process_remotes(
				session,
				process,
				arg.lease,
				arg.tokens.clone(),
				&locations.remotes,
				arg.source,
			)
			.await
			.map_err(
				|error| tg::error!(!error, %process, "failed to wait for the process on the remote"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(future))
	}

	pub(super) async fn try_wait_process_runner(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: &tg::process::wait::Arg,
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>,
			tg::Location,
		)>,
	> {
		let Some(runner) = Self::try_get_process_runner_inner(session, id, arg.location.as_ref())
		else {
			return Ok(None);
		};
		let mut requested = tg::authorization::permission::process::Set::NODE;
		requested.insert(tg::authorization::permission::process::Set::NODE_ERROR_OBJECTS);
		requested.insert(tg::authorization::permission::process::Set::NODE_OUTPUT_OBJECTS);
		let Some(tg::authorization::permission::Set::Process(permissions)) = self
			.authorize_process_runner(session, id, &arg.tokens, requested)
			.await?
		else {
			return Ok(None);
		};
		let location = runner.location.clone();
		let component = self.clone();
		let session = session.clone();
		let id = id.clone();
		let arg = arg.clone();
		let future = async move {
			component
				.try_wait_process_runner_task(&session, &id, arg, runner, permissions)
				.await
		}
		.boxed();
		Ok(Some((future, location)))
	}

	fn try_wait_process_runner_task<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		mut arg: tg::process::wait::Arg,
		mut runner: crate::Runner,
		permissions: tg::authorization::permission::process::Set,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::outcome::Data>>> {
		async move {
			loop {
				let outcome = runner
					.processes
					.get(id)
					.map(|process| -> tg::Result<_> {
						if !process.data.status.is_finished() {
							return Ok(None);
						}
						let outcome = Self::create_process_outcome_data(
							&process.data,
							permissions,
							false,
							None,
							&runner.location,
						)?;
						Ok(Some(outcome))
					})
					.transpose()?;
				let Some(outcome) = outcome else {
					arg.location = Some(runner.location_arg);
					let Some(future) = self
						.try_wait_process_future(session, &tg::Either::Right(id.clone()), arg)
						.boxed()
						.await?
					else {
						return Ok(None);
					};
					return future.await;
				};
				if let Some(mut outcome) = outcome {
					// The runner has the outcome locally, but the process still belongs to its original location.
					if runner.location.is_remote() {
						let location = tg::Location::Local(tg::location::Local::default());
						Self::update_outcome_referents_for_location(
							session,
							&mut outcome,
							&location,
							false,
						)?;
					}
					return Ok(Some(outcome));
				}
				runner.changed.changed().await.ok();
			}
		}
		.boxed()
	}

	fn create_process_outcome_data(
		data: &tg::process::Data,
		permissions: tg::authorization::permission::process::Set,
		retain_stored_sync: bool,
		sync: Option<&tg::Referent<tg::sync::Id>>,
		location: &tg::Location,
	) -> tg::Result<tg::process::outcome::Data> {
		let exit = data
			.exit
			.ok_or_else(|| tg::error!("expected the exit to be set"))?;
		let mut outcome = tg::process::outcome::Data {
			checksum: data.checksum.actual.clone(),
			error: data.error_with_location_and_tokens(),
			exit,
			output: data.output_with_location_and_tokens(),
		};

		// A result sync can expose both fields, so require permission for every object-bearing field.
		let required = Self::outcome_sync_permissions(&outcome);
		let authorize_sync = permissions.contains(required);
		let retain_sync = authorize_sync && retain_stored_sync;
		outcome.error = outcome.error.map(|error| match error {
			tg::Either::Left(error) => tg::Either::Left(error.without_location_and_tokens()),
			tg::Either::Right(mut error) => {
				if permissions
					.contains(tg::authorization::permission::process::Set::NODE_ERROR_OBJECTS)
				{
					Self::retain_wait_object_tokens(
						&mut error.options.tokens,
						&error.node.clone().into(),
						retain_sync,
					);
				} else {
					error.options.clear_location_and_tokens();
				}
				tg::Either::Right(error)
			},
		});
		outcome.output = outcome.output.map(|mut value| {
			if permissions
				.contains(tg::authorization::permission::process::Set::NODE_OUTPUT_OBJECTS)
			{
				tangram_server_process_core::update_process_value_tokens(
					&mut value,
					&mut |tokens, id| {
						Self::retain_wait_object_tokens(tokens, id, retain_sync);
					},
				);
				value
			} else {
				value.without_location_and_tokens()
			}
		});
		if authorize_sync && let Some(sync) = sync {
			Self::update_outcome_authorization_tokens_for_sync(&mut outcome, sync, location);
		}

		Ok(outcome)
	}

	pub(super) fn retain_wait_object_tokens(
		tokens: &mut tg::authorization::Tokens,
		id: &tg::object::Id,
		retain_sync: bool,
	) {
		// An inherited capability can cover objects outside this result.
		let original = std::mem::take(tokens);
		for (location, entry) in original.iter() {
			for token in &entry.authorization {
				if token.body.resource == tg::Id::from(id.clone())
					|| retain_sync
						&& token.body.resource.kind() == tg::id::Kind::Sync
						&& token.body.authorizes(tg::authorization::Permission::Sync(
							tg::authorization::permission::sync::Permission::Read,
						)) {
					tokens.insert_authorization(location.clone(), token.clone());
				}
			}
		}
	}

	fn outcome_sync_permissions(
		outcome: &tg::process::outcome::Data,
	) -> tg::authorization::permission::process::Set {
		let mut permissions = tg::authorization::permission::process::Set::empty();
		if matches!(outcome.error, Some(tg::Either::Right(_))) {
			permissions.insert(tg::authorization::permission::process::Set::NODE_ERROR_OBJECTS);
		}
		let mut objects = std::collections::BTreeSet::new();
		if let Some(value) = &outcome.output {
			value.children(&mut objects);
		}
		if !objects.is_empty() {
			permissions.insert(tg::authorization::permission::process::Set::NODE_OUTPUT_OBJECTS);
		}
		permissions
	}

	fn update_outcome_authorization_tokens_for_sync(
		outcome: &mut tg::process::outcome::Data,
		sync: &tg::Referent<tg::sync::Id>,
		location: &tg::Location,
	) {
		let mut authorization_tokens = tg::authorization::Tokens::default();
		for token in sync.options.tokens.local_authorization() {
			authorization_tokens.insert_authorization(location.clone(), token.clone());
		}
		if let Some(tg::Either::Right(error)) = &mut outcome.error {
			error.options.tokens.inherit(&authorization_tokens);
		}
		if let Some(data) = &mut outcome.output {
			tangram_server_process_core::update_process_value_tokens(data, &mut |tokens, _| {
				tokens.inherit(&authorization_tokens);
			});
		}
	}

	pub(super) async fn try_wait_process_local(
		&self,
		session: &Session,
		id: &tg::process::Id,
		tokens: Vec<tg::authorization::Token>,
		source: tg::process::Source,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>>> {
		let mut wakeups = self
			.create_process_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;
		let mut requested = tg::authorization::permission::process::Set::NODE;
		requested.insert(tg::authorization::permission::process::Set::NODE_ERROR_OBJECTS);
		requested.insert(tg::authorization::permission::process::Set::NODE_OUTPUT_OBJECTS);
		let deadline = session.server.control_read_deadline();
		let process = loop {
			tokio::select! {
				output = self.try_get_process_observation_local_with_permissions(session, id, &tokens, source, deadline, requested) => break output?,
				wakeup = wakeups.next() => {
					if wakeup.is_none() { return Ok(None); }
				},
			}
		};
		let Some((process, permissions)) = process else {
			return Ok(None);
		};

		let mut stream = self.create_process_data_stream_local(
			session,
			id,
			Some(process),
			Some(wakeups),
			source,
		);
		let future = async move {
			let process = loop {
				let process = stream.try_next().await?.ok_or_else(|| {
					tg::error!("the process status stream ended before the process finished")
				})?;
				if process.status.is_finished() {
					break process;
				}
			};
			let location = tg::Location::Local(tg::location::Local::default());
			let outcome =
				Self::create_process_outcome_data(&process, permissions, true, None, &location)?;
			Ok(Some(outcome))
		};

		Ok(Some(future.boxed()))
	}

	async fn try_wait_process_regions(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		lease: Option<tg::process::Lease>,
		tokens: tg::authorization::Tokens,
		regions: &[String],
		source: tg::process::Source,
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>,
			String,
		)>,
	> {
		let mut futures = regions
			.iter()
			.map(|region| {
				self.try_wait_process_region(
					session,
					process,
					lease,
					tokens.clone(),
					region,
					source,
				)
			})
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(future)) => {
					result = Ok(Some(future));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(future) = result? else {
			return Ok(None);
		};
		Ok(Some(future))
	}

	async fn try_wait_process_region(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		lease: Option<tg::process::Lease>,
		tokens: tg::authorization::Tokens,
		region: &str,
		source: tg::process::Source,
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>,
			String,
		)>,
	> {
		let client = session
			.get_region_session_for_process(region)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the region client"),
			)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let tokens = tokens.for_location(&location);
		let arg = tg::process::wait::Arg {
			lease,
			location: Some(location.clone().into()),
			source,
			tokens,
		};
		let Some(future) = client.try_wait_process_future(process, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to wait for the process"),
		)?
		else {
			return Ok(None);
		};
		let future = Self::update_wait_process_referents_for_location(
			session,
			future.boxed(),
			location,
			false,
		);
		Ok(Some((future, region.to_owned())))
	}

	async fn try_wait_process_remotes(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		lease: Option<tg::process::Lease>,
		tokens: tg::authorization::Tokens,
		remotes: &[tangram_server_core::location::Remote],
		source: tg::process::Source,
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>,
			tangram_server_core::location::Remote,
		)>,
	> {
		let mut futures = remotes
			.iter()
			.map(|remote| {
				self.try_wait_process_remote(
					session,
					process,
					lease,
					tokens.clone(),
					remote,
					source,
				)
			})
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(future)) => {
					result = Ok(Some(future));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(future) = result? else {
			return Ok(None);
		};
		Ok(Some(future))
	}

	async fn try_wait_process_remote(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		lease: Option<tg::process::Lease>,
		tokens: tg::authorization::Tokens,
		remote: &tangram_server_core::location::Remote,
		source: tg::process::Source,
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>,
			tangram_server_core::location::Remote,
		)>,
	> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session_for_process(session, &remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let trusted = client.trusted();
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let tokens = tokens.for_location(&location);
		let arg = tg::process::wait::Arg {
			lease,
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			source,
			tokens,
		};
		let Some(future) = client.try_wait_process_future(process, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to wait for the process"),
		)?
		else {
			return Ok(None);
		};
		let future = Self::update_wait_process_referents_for_location(
			session,
			future.boxed(),
			location,
			trusted,
		);
		Ok(Some((future, remote.clone())))
	}

	fn update_wait_process_referents_for_location(
		session: &Session,
		future: BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>,
		location: tg::Location,
		trusted: bool,
	) -> BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>> {
		let session = session.clone();
		async move {
			let mut outcome = future.await?;
			if let Some(outcome) = &mut outcome {
				Self::update_outcome_referents_for_location(&session, outcome, &location, trusted)?;
			}
			Ok(outcome)
		}
		.boxed()
	}

	pub(super) fn update_outcome_referents_for_location(
		session: &Session,
		outcome: &mut tg::process::outcome::Data,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		if let Some(error) = &mut outcome.error {
			match error {
				tg::Either::Left(error) => {
					session
						.server
						.components
						.authorization
						.update_error_data_referents_for_location(
							session, error, location, trusted,
						)?;
				},
				tg::Either::Right(error) => {
					session
						.server
						.components
						.authorization
						.update_tokens_and_location(
							session,
							&mut error.options.tokens,
							Some(&mut error.options.location),
							location,
							trusted,
						)?;
				},
			}
		}
		if let Some(value) = &mut outcome.output {
			session
				.server
				.components
				.authorization
				.update_value_data_referents_for_location(session, value, location, trusted)?;
		}
		Ok(())
	}

	pub(super) fn attach_wait_process_guard(
		session: &Session,
		id: &tg::process::Id,
		arg: &tg::process::wait::Arg,
		location: Option<tg::location::Arg>,
		cancel: Arc<AtomicBool>,
		future: BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>,
	) -> BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>> {
		// Remove the parent's child leases when the child finishes.
		let future = if matches!(session.context.principal, tg::Principal::Process(_)) {
			let session = session.clone();
			let child = id.clone();
			async move {
				let result = future.await;
				if matches!(&result, Ok(Some(_))) {
					Self::remove_finished_process_child_lease(&session, &child);
				}
				result
			}
			.boxed()
		} else {
			future
		};

		// If a lease is provided, attach a cancellation guard.
		if let Some(lease) = arg.lease {
			let future = {
				let cancel = cancel.clone();
				let stopper = session.context.stopper.clone();
				async move {
					let result = future.await;
					// Suppress cancellation when the wait returns during shutdown.
					if matches!(&result, Ok(Some(_)))
						|| stopper.as_ref().is_some_and(Stopper::stopped)
					{
						cancel.store(false, Ordering::SeqCst);
					}
					result
				}
			}
			.boxed();

			// Release the lease even if the request's server is shutting down.
			let mut release_session = session.clone();
			release_session.context.stopper = None;
			let checkpoint_id = id.clone();
			let id = id.clone();
			let server = release_session.server.clone();
			let guard = scopeguard::guard((), move |()| {
				// Cancel the process if the wait did not complete, after a checkpoint that lets tests observe the decision.
				let cancel = cancel.load(Ordering::SeqCst);
				if !cancel && server.checkpoints.is_none() {
					return;
				}
				let arg = tg::process::cancel::Arg {
					location: location.clone(),
					lease,
				};
				tokio::spawn(async move {
					tangram_server_core::checkpoint!(server, "process.wait.release", process = %id, cancel = %cancel)
						.await;
					if cancel {
						release_session
							.cancel_process(&tg::Either::Right(id.clone()), arg)
							.await
							.ok();
					}
				});
			});
			let server = session.server.clone();
			let future = async move {
				tangram_server_core::checkpoint!(server, "process.wait.attach", process = %checkpoint_id).await;

				future.await
			}
			.boxed();

			future.attach(guard).boxed()
		} else {
			future
		}
	}

	pub(super) fn remove_finished_process_child_lease(session: &Session, child: &tg::process::Id) {
		let tg::Principal::Process(parent) = &session.context.principal else {
			return;
		};
		session
			.server
			.components
			.runner
			.state()
			.try_update_process(parent, |process| {
				if let Some(child) = process.children.get_mut(child) {
					child.lease = None;
					child.location = None;
				}
			});
	}

	pub(crate) async fn try_wait_process_future_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		process: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the process.
		let process = process
			.parse::<tg::Either<tg::process::Nonce, tg::process::Id>>()
			.map_err(|_| tg::error!(argument, "failed to parse the process"))?;

		// Parse the arg.
		let (arg, request) = request
			.arg::<tg::process::wait::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the stream.
		let Some(stream) = self.try_wait_process_stream(session, &process, arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the body.
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

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
