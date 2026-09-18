use {
	crate::Session,
	futures::{
		FutureExt as _, StreamExt as _,
		future::{self, BoxFuture},
		stream::{self, FuturesUnordered},
	},
	std::sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	tangram_client::prelude::*,
	tangram_futures::{future::Ext as _, stream::TryExt as _, task::Stopper},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::Index as _,
};

impl Session {
	pub async fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>> {
		self.try_wait_process_future_with_cancel(id, arg, Arc::new(AtomicBool::new(true)))
			.await
	}

	pub(crate) async fn create_wait_process_finished_future_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<BoxFuture<'static, tg::Result<()>>> {
		if let Some(mut runner) = self.try_get_process_runner_including_finished(id, None) {
			let id = id.clone();
			let future = async move {
				loop {
					if runner
						.processes
						.get(&id)
						.is_none_or(|process| process.data.status.is_finished())
					{
						break;
					}
					if runner.changed.changed().await.is_err() {
						break;
					}
				}
				Ok(())
			}
			.boxed();
			return Ok(future);
		}
		let mut stream = self
			.create_process_status_stream_local(id, None, None)
			.await?;
		let future = async move {
			while let Some(event) = stream.next().await {
				match event? {
					tg::process::status::Event::End => break,
					tg::process::status::Event::Status(status) => {
						if status.is_finished() {
							return Ok(());
						}
					},
				}
			}

			Err(tg::error!(
				"the process status stream ended before the process finished"
			))
		}
		.boxed();

		Ok(future)
	}

	async fn try_wait_process_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<
		Option<impl futures::Stream<Item = tg::Result<tg::process::wait::Event>> + Send + use<>>,
	> {
		let Some(future) = self.try_wait_process_future(id, arg).await? else {
			return Ok(None);
		};
		let stream = stream::once(future).filter_map(|result| async move {
			match result {
				Ok(Some(value)) => Some(Ok(tg::process::wait::Event::Output(value))),
				Ok(None) => None,
				Err(error) => Some(Err(error)),
			}
		});
		Ok(Some(stream))
	}

	pub(super) async fn try_wait_process_future_with_cancel(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
		cancel: Arc<AtomicBool>,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>> {
		// This session owns cancellation; downstream waits only observe the process.
		let mut observe_arg = arg.clone();
		observe_arg.lease = None;
		let output = match self.try_wait_process_runner(id, &observe_arg).await? {
			Some(output) => Some(output),
			None => self.try_wait_process_inner(id, observe_arg).await?,
		};
		let Some((future, location)) = output else {
			return Ok(None);
		};
		let future =
			self.attach_wait_process_guard(id, &arg, Some(location.into()), cancel, future);
		Ok(Some(future))
	}

	pub(super) async fn try_wait_process_runner(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::wait::Arg,
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
			tg::Location,
		)>,
	> {
		let Some(runner) =
			self.try_get_process_runner_including_finished(id, arg.location.as_ref())
		else {
			return Ok(None);
		};
		let mut requested = tg::authorization::permission::process::Set::NODE;
		requested.insert(tg::authorization::permission::process::Set::NODE_ERROR);
		requested.insert(tg::authorization::permission::process::Set::NODE_OUTPUT);
		let Some(tg::authorization::permission::Set::Process(permissions)) = self
			.authorize_process_runner(id, &arg.tokens, requested)
			.await?
		else {
			return Ok(None);
		};
		let location = runner.location.clone();
		let session = self.clone();
		let id = id.clone();
		let arg = arg.clone();
		let future = async move {
			session
				.try_wait_process_runner_task(&id, arg, runner, permissions)
				.await
		}
		.boxed();
		Ok(Some((future, location)))
	}

	async fn try_wait_process_runner_task(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::wait::Arg,
		mut runner: crate::process::Runner,
		permissions: tg::authorization::permission::process::Set,
	) -> tg::Result<Option<tg::process::wait::Output>> {
		loop {
			let output = runner
				.processes
				.get(id)
				.map(|process| -> tg::Result<_> {
					if !process.data.status.is_finished() {
						return Ok(None);
					}
					let output = Self::create_process_wait_output_runner(
						&process.data,
						permissions,
						process.sync.as_ref(),
						&runner.location,
					)?;
					Ok(Some(output))
				})
				.transpose()?;
			let Some(output) = output else {
				arg.location = Some(runner.location_arg);
				let Some((future, _)) = self.try_wait_process_inner(id, arg).await? else {
					return Ok(None);
				};
				return future.await;
			};
			if let Some(mut output) = output {
				// The runner has the output locally, but the process still belongs to its original location.
				if runner.location.is_remote() {
					let location = tg::Location::Local(tg::location::Local::default());
					self.update_wait_output_referents_for_location(&mut output, &location, false)?;
				}
				return Ok(Some(output));
			}
			runner.changed.changed().await.ok();
		}
	}

	fn create_process_wait_output_runner(
		data: &tg::process::Data,
		permissions: tg::authorization::permission::process::Set,
		sync: Option<&tg::sync::Token>,
		location: &tg::Location,
	) -> tg::Result<tg::process::wait::Output> {
		let exit = data
			.exit
			.ok_or_else(|| tg::error!("expected the exit to be set"))?;
		let error = data.error.clone().map(|error| match error {
			tg::Either::Left(error) => tg::Either::Left(error.without_location_and_tokens()),
			tg::Either::Right(mut error) => {
				if permissions.contains(tg::authorization::permission::process::Set::NODE_ERROR) {
					Self::retain_wait_object_tokens(
						&mut error.options.tokens,
						&error.node.clone().into(),
					);
				} else {
					error.options.clear_location_and_tokens();
				}
				tg::Either::Right(error)
			},
		});
		let output = data.output.clone().map(|mut output| {
			if permissions.contains(tg::authorization::permission::process::Set::NODE_OUTPUT) {
				Self::update_wait_value_tokens(&mut output, &mut Self::retain_wait_object_tokens);
				output
			} else {
				output.without_location_and_tokens()
			}
		});
		let mut output = tg::process::wait::Output {
			error,
			exit,
			output,
		};

		// The result sync covers every object in both fields.
		let required = Self::wait_output_sync_permissions(&output);
		if permissions.contains(required)
			&& let Some(sync) = sync
		{
			Self::update_wait_output_sync_token(&mut output, sync, location);
		}

		Ok(output)
	}

	fn update_wait_value_tokens(
		data: &mut tg::value::Data,
		update: &mut impl FnMut(&mut tg::Tokens, &tg::object::Id),
	) {
		match data {
			tg::value::Data::Array(array) => {
				for value in array {
					Self::update_wait_value_tokens(value, update);
				}
			},
			tg::value::Data::Bool(_)
			| tg::value::Data::Bytes(_)
			| tg::value::Data::Null
			| tg::value::Data::Number(_)
			| tg::value::Data::Placeholder(_)
			| tg::value::Data::String(_) => {},
			tg::value::Data::Map(map) => {
				for value in map.values_mut() {
					Self::update_wait_value_tokens(value, update);
				}
			},
			tg::value::Data::Module(module) => {
				let mut objects = std::collections::BTreeSet::new();
				module.children(&mut objects);
				if let Some(id) = objects.first() {
					update(&mut module.referent.options.tokens, id);
				} else {
					module.referent.options.clear_location_and_tokens();
				}
			},
			tg::value::Data::Mutation(mutation) => match mutation {
				tg::mutation::Data::Append { values } | tg::mutation::Data::Prepend { values } => {
					for value in values {
						Self::update_wait_value_tokens(value, update);
					}
				},
				tg::mutation::Data::Merge { value } => {
					for value in value.values_mut() {
						Self::update_wait_value_tokens(value, update);
					}
				},
				tg::mutation::Data::Prefix { template, .. }
				| tg::mutation::Data::Suffix { template, .. } => {
					Self::update_wait_template_tokens(template, update);
				},
				tg::mutation::Data::Set { value } | tg::mutation::Data::SetIfUnset { value } => {
					Self::update_wait_value_tokens(value, update);
				},
				tg::mutation::Data::Unset => {},
			},
			tg::value::Data::Object(object) => update(&mut object.options.tokens, &object.node),
			tg::value::Data::Template(template) => {
				Self::update_wait_template_tokens(template, update);
			},
		}
	}

	fn update_wait_template_tokens(
		template: &mut tg::template::Data,
		update: &mut impl FnMut(&mut tg::Tokens, &tg::object::Id),
	) {
		for component in &mut template.components {
			if let tg::template::data::Component::Artifact(artifact) = component {
				update(&mut artifact.options.tokens, &artifact.node.clone().into());
			}
		}
	}

	fn retain_wait_object_tokens(tokens: &mut tg::Tokens, id: &tg::object::Id) {
		// An inherited capability can cover objects outside this result.
		let original = std::mem::take(tokens);
		for (location, entry) in original.iter() {
			for token in &entry.authorization {
				if token.body.resource == tg::Id::from(id.clone()) {
					tokens.insert_authorization(location.clone(), token.clone());
				}
			}
		}
	}

	fn wait_output_sync_permissions(
		output: &tg::process::wait::Output,
	) -> tg::authorization::permission::process::Set {
		let mut permissions = tg::authorization::permission::process::Set::empty();
		if matches!(output.error, Some(tg::Either::Right(_))) {
			permissions.insert(tg::authorization::permission::process::Set::NODE_ERROR);
		}
		let mut objects = std::collections::BTreeSet::new();
		if let Some(output) = &output.output {
			output.children(&mut objects);
		}
		if !objects.is_empty() {
			permissions.insert(tg::authorization::permission::process::Set::NODE_OUTPUT);
		}
		permissions
	}

	fn update_wait_output_sync_token(
		output: &mut tg::process::wait::Output,
		sync: &tg::sync::Token,
		location: &tg::Location,
	) {
		if let Some(tg::Either::Right(error)) = &mut output.error {
			error
				.options
				.tokens
				.set_sync(location.clone(), sync.clone());
		}
		if let Some(data) = &mut output.output {
			Self::update_wait_value_tokens(data, &mut |tokens, _| {
				tokens.set_sync(location.clone(), sync.clone());
			});
		}
	}

	async fn try_wait_process_inner(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
			tg::Location,
		)>,
	> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		if let Some(local) = &locations.local {
			if local.current
				&& let Some(future) = self
					.try_wait_process_local(id, arg.tokens.local_authorization().to_vec())
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?
			{
				let location = tg::Location::Local(tg::location::Local::default());
				return Ok(Some((future, location)));
			}

			if let Some((future, region)) = self
				.try_wait_process_regions(id, arg.lease.clone(), arg.tokens.clone(), &local.regions)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to wait for the process in another region"),
				)? {
				let location = tg::Location::Local(tg::location::Local {
					region: Some(region),
				});
				return Ok(Some((future, location)));
			}
		}

		let Some((future, remote)) = self
			.try_wait_process_remotes(
				id,
				arg.lease.clone(),
				arg.tokens.clone(),
				&locations.remotes,
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

		Ok(Some((future, location)))
	}

	pub(super) async fn try_wait_process_local(
		&self,
		id: &tg::process::Id,
		tokens: Vec<tg::authorization::Token>,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>> {
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.clone());
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		let mut wakeups = self
			.create_process_status_wakeup_stream(id, None, None)
			.await?;
		let authorize_future = self.authorize(resource, permission).boxed();
		let get_future = self
			.try_get_process_local_inner_with_wakeups(id, false, &mut wakeups)
			.boxed();
		let (permissions, process) = future::try_join(authorize_future, get_future).await?;
		let Some(permissions) = permissions else {
			return Ok(None);
		};
		let Some(process) = process else {
			return Ok(None);
		};
		if !permissions.contains(permission) {
			return Ok(None);
		}
		if !process.data.status.is_finished()
			&& process
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
		{
			return Ok(None);
		}
		let initial = process.data.status;
		let stream =
			self.create_process_status_stream_local_with_wakeups(id, Some(initial), Some(wakeups));
		let session = self.clone();
		let id = id.clone();
		let stream = stream.boxed();
		let future = async move {
			let stream = stream
				.take_while(|event| {
					future::ready(!matches!(event, Ok(tg::process::status::Event::End)))
				})
				.map(|event| match event {
					Ok(tg::process::status::Event::Status(status)) => Ok(status),
					Err(error) => Err(error),
					_ => unreachable!(),
				});
			let status = stream
				.try_last()
				.await?
				.ok_or_else(|| tg::error!("failed to get the status"))?;
			if !status.is_finished() {
				return Err(tg::error!("expected the process to be finished"));
			}
			let process = session
				.get_process_local(&id, false)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
			let exit = process
				.data
				.exit
				.ok_or_else(|| tg::error!("expected the exit to be set"))?;
			let mut output = tg::process::wait::Output {
				error: process
					.data
					.error
					.map(|error| error.map_right(|error| error)),
				exit,
				output: process.data.output,
			};
			session
				.add_wait_output_sync_token(&id, tokens, &mut output)
				.await?;
			Ok(Some(output))
		};

		Ok(Some(future.boxed()))
	}

	async fn add_wait_output_sync_token(
		&self,
		id: &tg::process::Id,
		tokens: Vec<tg::authorization::Token>,
		output: &mut tg::process::wait::Output,
	) -> tg::Result<()> {
		let Some(process) = self.server.index.try_get_process(id).await? else {
			return Ok(());
		};
		let mut requested = Self::wait_output_sync_permissions(output);
		let missing = (requested.contains(tg::authorization::permission::process::Set::NODE_ERROR)
			&& !process.storage.node_error)
			|| (requested.contains(tg::authorization::permission::process::Set::NODE_OUTPUT)
				&& !process.storage.node_output);
		if !missing {
			return Ok(());
		}

		// The shared result sync can confer both fields before their grants reach the index.
		requested.insert(tg::authorization::permission::process::Set::NODE);
		let requested = tg::authorization::permission::Set::Process(requested);
		let required = tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::NODE,
		);
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens);
		let mut permissions = self
			.authorize_batch_with_required([(resource, requested)], required)
			.await?;
		if !permissions
			.pop()
			.flatten()
			.is_some_and(|permissions| permissions.contains(requested))
		{
			return Ok(());
		}

		// A completed transfer no longer needs its live control connection.
		let timeout = std::time::Duration::from_secs(1);
		if let Ok(Ok(control)) =
			tokio::time::timeout(timeout, self.get_process_control_output(id)).await
			&& let Some(sync) = control.sync
		{
			let location = tg::Location::Local(tg::location::Local::default());
			Self::update_wait_output_sync_token(output, &sync, &location);
		}

		Ok(())
	}

	async fn try_wait_process_regions(
		&self,
		id: &tg::process::Id,
		lease: Option<String>,
		tokens: tg::Tokens,
		regions: &[String],
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
			String,
		)>,
	> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_wait_process_region(id, lease.clone(), tokens.clone(), region))
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
		id: &tg::process::Id,
		lease: Option<String>,
		tokens: tg::Tokens,
		region: &str,
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
			String,
		)>,
	> {
		let client = self.get_region_session_for_process(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let tokens = tokens.for_location(&location);
		let arg = tg::process::wait::Arg {
			lease,
			location: Some(location.clone().into()),
			tokens,
		};
		let Some(future) = client.try_wait_process_future(id, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to wait for the process"),
		)?
		else {
			return Ok(None);
		};
		let future =
			self.update_wait_process_referents_for_location(future.boxed(), location, false);
		Ok(Some((future, region.to_owned())))
	}

	async fn try_wait_process_remotes(
		&self,
		id: &tg::process::Id,
		lease: Option<String>,
		tokens: tg::Tokens,
		remotes: &[crate::location::Remote],
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
			crate::location::Remote,
		)>,
	> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_wait_process_remote(id, lease.clone(), tokens.clone(), remote))
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
		id: &tg::process::Id,
		lease: Option<String>,
		tokens: tg::Tokens,
		remote: &crate::location::Remote,
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
			crate::location::Remote,
		)>,
	> {
		let client = self
			.get_remote_session_for_process(&remote.name)
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
			tokens,
		};
		let Some(future) = client.try_wait_process_future(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to wait for the process"),
		)?
		else {
			return Ok(None);
		};
		let future =
			self.update_wait_process_referents_for_location(future.boxed(), location, trusted);
		Ok(Some((future, remote.clone())))
	}

	fn update_wait_process_referents_for_location(
		&self,
		future: BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
		location: tg::Location,
		trusted: bool,
	) -> BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>> {
		let session = self.clone();
		async move {
			let mut output = future.await?;
			if let Some(output) = &mut output {
				session.update_wait_output_referents_for_location(output, &location, trusted)?;
			}
			Ok(output)
		}
		.boxed()
	}

	pub(super) fn update_wait_output_referents_for_location(
		&self,
		output: &mut tg::process::wait::Output,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		if let Some(error) = &mut output.error {
			match error {
				tg::Either::Left(error) => {
					self.update_error_data_referents_for_location(error, location, trusted)?;
				},
				tg::Either::Right(error) => {
					self.update_tokens_and_location(
						&mut error.options.tokens,
						Some(&mut error.options.location),
						location,
						trusted,
					)?;
				},
			}
		}
		if let Some(value) = &mut output.output {
			self.update_value_data_referents_for_location(value, location, trusted)?;
		}
		Ok(())
	}

	pub(super) fn attach_wait_process_guard(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::wait::Arg,
		location: Option<tg::location::Arg>,
		cancel: Arc<AtomicBool>,
		future: BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
	) -> BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>> {
		// Remove the parent's child leases when the child finishes.
		let future = if matches!(self.context.principal, tg::Principal::Process(_)) {
			let session = self.clone();
			let child = id.clone();
			async move {
				let output = future.await;
				if matches!(&output, Ok(Some(_))) {
					session.remove_finished_process_child_lease(&child);
				}
				output
			}
			.boxed()
		} else {
			future
		};

		// If a lease is provided, attach a cancellation guard.
		if let Some(lease) = arg.lease.clone() {
			let stopper = self.context.stopper.clone();
			let future = {
				let cancel = cancel.clone();
				async move {
					let output = future.await;
					if matches!(&output, Ok(Some(_))) {
						cancel.store(false, Ordering::SeqCst);
					}
					output
				}
			}
			.boxed();

			let session = self.clone();
			let checkpoint_id = id.clone();
			let id = id.clone();
			let guard = scopeguard::guard((), move |()| {
				if cancel.load(Ordering::SeqCst) && !stopper.as_ref().is_some_and(Stopper::stopped)
				{
					let arg = tg::process::cancel::Arg {
						location: location.clone(),
						lease,
					};
					tokio::spawn(async move {
						session.cancel_process(&id, arg).await.ok();
					});
				}
			});
			let server = self.server.clone();
			let future = async move {
				crate::checkpoint!(server, "process.wait.attach", process = %checkpoint_id).await;

				future.await
			}
			.boxed();

			future.attach(guard).boxed()
		} else {
			future
		}
	}

	pub(super) fn remove_finished_process_child_lease(&self, child: &tg::process::Id) {
		let tg::Principal::Process(parent) = &self.context.principal else {
			return;
		};
		self.server
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
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the ID.
		let id = id
			.parse::<tg::process::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

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
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the stream.
		let Some(stream) = self.try_wait_process_stream(&id, arg).await? else {
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
