use {
	crate::Component,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream, FuturesUnordered},
	},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
	tangram_server_core::{
		Session,
		location::await_local_future_over_regions_remotes_futures,
		process::{TryGetProcessRegionsArg, TryGetProcessRemotesArg},
	},
};

mod token;

struct TryGetProcessRegionArg<'a> {
	availability: bool,
	id: &'a tg::process::Id,
	metadata: bool,
	region: &'a str,
	tokens: &'a tg::authorization::Tokens,
}

struct TryGetProcessRemoteArg<'a> {
	availability: bool,
	id: &'a tg::process::Id,
	metadata: bool,
	remote: &'a tangram_server_core::location::Remote,
	tokens: &'a tg::authorization::Tokens,
}

impl Component {
	pub async fn try_get_process(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let runner = self
			.try_get_process_runner(session, id, &arg)
			.boxed()
			.await?;
		if let Some(output) = &runner
			&& !output.data.status.is_finished()
		{
			return Ok(runner);
		}
		let mut arg = arg;
		if let Some(output) = &runner {
			arg.location = output.location.clone().map(Into::into);
		}

		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		let current = locations.local.as_ref().is_some_and(|local| local.current);

		// Read the process and authorize it without a search, and then with a search, with one request ID, so that the process's holder answers both reads once.
		let request_id = tangram_server_core::control::id();
		let node = if current {
			self.try_get_process_local_inner(session, id, request_id.clone(), arg.metadata, false)
				.boxed()
				.await?
		} else {
			None
		};
		if current
			&& let Some(output) = self
				.try_get_process_local(
					session,
					id,
					request_id.clone(),
					arg.metadata,
					arg.availability,
					arg.tokens.local_authorization(),
					node.clone(),
					false,
				)
				.await?
		{
			let output = if output.data.status.is_finished() {
				output
			} else {
				runner.unwrap_or(output)
			};
			return Ok(Some(output));
		}

		// Search locally while trying the regions and then the remotes.
		let local_future = async {
			if !current {
				return Ok(None);
			}
			self.try_get_process_local(
				session,
				id,
				request_id,
				arg.metadata,
				arg.availability,
				arg.tokens.local_authorization(),
				node,
				true,
			)
			.await
		};
		let regions_and_remotes_future = async {
			if let Some(local) = &locations.local {
				let regions_arg = TryGetProcessRegionsArg {
					availability: arg.availability,
					id,
					metadata: arg.metadata,
					regions: &local.regions,
					tokens: &arg.tokens,
				};
				if let Some(output) = self
					.try_get_process_regions(session, regions_arg)
					.await
					.map_err(
						|error| tg::error!(!error, %id, "failed to get the process from another region"),
					)? {
					return Ok(Some(output));
				}
			}
			let remotes_arg = TryGetProcessRemotesArg {
				availability: arg.availability,
				id,
				metadata: arg.metadata,
				remotes: &locations.remotes,
				tokens: &arg.tokens,
			};
			self.try_get_process_remotes(session, remotes_arg)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the process from a remote"))
		};
		let output = await_local_future_over_regions_remotes_futures(
			local_future,
			regions_and_remotes_future,
		)
		.boxed()
		.await?;
		let Some(output) = output else {
			return Ok(runner);
		};
		let output = if output.data.status.is_finished() {
			output
		} else {
			runner.unwrap_or(output)
		};

		Ok(Some(output))
	}

	async fn try_get_process_runner(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: &tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let Some(runner) = Self::try_get_process_runner_inner(session, id, arg.location.as_ref())
		else {
			return Ok(None);
		};
		let authorization = self
			.authorize_process_get(
				session,
				id,
				arg.metadata,
				arg.availability,
				arg.tokens.local_authorization(),
				true,
			)
			.await?;
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		if !authorization.permissions.contains(permission) {
			return Ok(None);
		}
		let Some(mut data) = runner.processes.get(id).map(|process| process.data()) else {
			return Ok(None);
		};
		if let Some(children) = &mut data.children {
			Self::mask_process_child_tokens_with_permissions(children, authorization.permissions);
		}

		let mut output =
			Self::create_process_get_output(session, id, data, Some(runner.location.clone()), None);
		output.tokens = arg.tokens.clone();
		output.tokens.remove_local_authorization();
		let permissions = authorization.permissions;
		Self::add_tokens_to_process_get_output(session, id, authorization, &mut output)?;

		// Read index-only fields at the process's original location.
		if arg.metadata || arg.availability {
			let locations = session.locations(Some(&runner.location_arg)).await?;
			let current = locations.local.as_ref().is_some_and(|local| local.current);
			let metadata_future = async {
				if !arg.metadata {
					return Ok(None);
				}
				if current {
					return self
						.try_get_process_metadata_local_with_permissions(session, id, permissions)
						.await;
				}
				let arg = tg::process::metadata::Arg {
					location: Some(runner.location_arg.clone()),
					tokens: arg.tokens.clone(),
				};
				self.try_get_process_metadata(session, id, arg).await
			};
			let availability_future = async {
				if !arg.availability {
					return Ok(None);
				}
				if current {
					return self
						.try_get_process_availability_local_with_permissions(
							session,
							id,
							permissions,
						)
						.await;
				}
				let arg = tg::process::availability::Arg {
					location: Some(runner.location_arg.clone()),
					tokens: arg.tokens.clone(),
				};
				self.try_get_process_availability(session, id, arg).await
			};
			(output.metadata, output.availability) =
				future::try_join(metadata_future, availability_future).await?;
		}

		Ok(Some(output))
	}

	pub async fn try_get_process_local_with_permissions(
		&self,
		session: &Session,
		id: &tg::process::Id,
		permissions: tg::authorization::permission::Set,
		metadata: bool,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let node = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		if !permissions.contains(node) {
			tracing::trace!(%id, principal = ?session.context.principal, "authorization denied");
			return Ok(None);
		}
		let Some(mut output) = self
			.try_get_process_local_inner(
				session,
				id,
				tangram_server_core::control::id(),
				metadata,
				true,
			)
			.await?
		else {
			return Ok(None);
		};
		if metadata {
			output.metadata = self
				.try_get_process_metadata_local_with_permissions(session, id, permissions)
				.await?;
		}
		Ok(Some(output))
	}

	#[expect(clippy::too_many_arguments)]
	pub async fn try_get_process_local(
		&self,
		session: &Session,
		id: &tg::process::Id,
		request_id: String,
		metadata: bool,
		availability: bool,
		tokens: &[tg::authorization::Token],
		node: Option<tg::process::get::Output>,
		exhaustive: bool,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);

		// Authorize the process.
		let authorization = self
			.authorize_process_get(session, id, metadata, availability, tokens, exhaustive)
			.boxed()
			.await?;
		if !authorization.permissions.contains(permission) {
			return Ok(None);
		}

		// Read the process if it was not provided.
		let mut output = match node {
			Some(node) => Some(node),
			None => {
				self.try_get_process_local_inner(session, id, request_id, metadata, exhaustive)
					.boxed()
					.await?
			},
		};

		// Wait for the data to arrive through a sync, then read the process again.
		if output.is_none() {
			let permissions = tg::authorization::permission::Set::Process(
				tg::authorization::permission::process::Set::empty(),
			);
			let storage = tg::storage::Set::Process(tg::process::storage::Set::NODE);
			if session
				.verify_with_exhaustive(resource, permissions, storage, exhaustive)
				.boxed()
				.await?
				.check_exhaustion()?
				.outcome
				!= tangram_server_core::authorization::Outcome::Satisfied
			{
				return Ok(None);
			}
			output = self
				.try_get_process_local_inner(
					session,
					id,
					tangram_server_core::control::id(),
					metadata,
					exhaustive,
				)
				.boxed()
				.await?;
		}

		// Create the output.
		let Some(mut output) = output else {
			return Ok(None);
		};
		if output.data.status.is_finished()
			&& output.data.children.is_none()
			&& session
				.server
				.index
				.try_get_process_children_count(id)
				.await?
				.is_some()
		{
			self.set_process_children_from_index(session, id, true, &mut output.data)
				.await?;
		}
		if let Some(children) = &mut output.data.children {
			Self::mask_process_child_tokens_with_permissions(children, authorization.permissions);
		}
		let permissions = authorization.permissions;
		Self::add_tokens_to_process_get_output(session, id, authorization, &mut output)?;
		if metadata {
			output.metadata = self
				.try_get_process_metadata_local_with_permissions(session, id, permissions)
				.await?;
		}
		if availability {
			output.availability = self
				.try_get_process_availability_local_with_permissions(session, id, permissions)
				.await?;
		}

		Ok(Some(output))
	}

	pub(crate) async fn authorize_process_get(
		&self,
		session: &Session,
		id: &tg::process::Id,
		metadata: bool,
		availability: bool,
		tokens: &[tg::authorization::Token],
		exhaustive: bool,
	) -> tg::Result<tangram_server_core::authorization::Output> {
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		let mut requested = tg::authorization::permission::process::Set::NODE;
		requested.insert(tg::authorization::permission::process::Set::PARENT);
		if metadata
			|| availability
			|| session.context.principal.is_root()
			|| session.context.principal == tg::Principal::Process(id.clone())
		{
			requested = tg::authorization::permission::process::Set::all();
		} else {
			for token in tokens {
				if token.body.resource != tg::Id::from(id.clone()) {
					continue;
				}
				let permissions = tg::authorization::permission::process::Set::all()
					.iter()
					.filter(|permission| {
						token
							.body
							.authorizes(tg::authorization::Permission::Process(*permission))
					})
					.collect::<Vec<_>>();
				requested.insert(permissions.into());
			}
		}
		session
			.authorize_with_permissions_with_exhaustive(
				resource,
				tg::authorization::permission::Set::Process(requested),
				permission.into(),
				tg::authorization::permission::Set::Process(
					tg::authorization::permission::process::Set::empty(),
				),
				None,
				exhaustive,
			)
			.await?
			.check_exhaustion()
	}

	fn process_get_token_expires_at(
		session: &Session,
		authorization_expires_at: Option<i64>,
	) -> tg::Result<i64> {
		let created_at = session.server.clock.unix_timestamp();
		let time_to_live = i64::try_from(
			session
				.server
				.config
				.process
				.permission_time_to_live
				.as_secs(),
		)
		.map_err(|error| tg::error!(!error, "failed to convert the permission ttl"))?;
		let expires_at = created_at
			.checked_add(time_to_live)
			.ok_or_else(|| tg::error!("the permission expiration overflowed"))?;
		let expires_at =
			authorization_expires_at.map_or(expires_at, |expiration| expiration.min(expires_at));
		Ok(expires_at)
	}

	pub async fn get_process_local(
		&self,
		session: &Session,
		id: &tg::process::Id,
		metadata: bool,
	) -> tg::Result<tg::process::get::Output> {
		let output = self
			.try_get_process_local_inner(
				session,
				id,
				tangram_server_core::control::id(),
				metadata,
				true,
			)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;

		Ok(output)
	}

	pub async fn set_process_children_from_index(
		&self,
		session: &Session,
		id: &tg::process::Id,
		children_set: bool,
		data: &mut tg::process::Data,
	) -> tg::Result<()> {
		if !children_set || data.children.is_some() {
			return Ok(());
		}
		let mut children = Vec::new();
		let mut position = 0;
		let length = 256;
		loop {
			let output = session
				.server
				.index
				.try_get_process_children(id, std::io::SeekFrom::Start(position), length)
				.await?
				.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
			let output_length = u64::try_from(output.len()).unwrap();
			children.extend(output);
			if output_length < length {
				break;
			}
			position += output_length;
		}
		data.children = Some(children);

		Ok(())
	}

	pub(crate) async fn try_get_process_local_inner(
		&self,
		session: &Session,
		id: &tg::process::Id,
		request_id: String,
		metadata: bool,
		exhaustive: bool,
	) -> tg::Result<Option<tg::process::get::Output>> {
		// Subscribe before reading to avoid missing a status change between the read and subscription.
		let mut wakeups = self
			.create_process_status_wakeup_stream(session, id, None, None)
			.await?;

		// Get the process from its holder or the index. A finished process requires the index, because the holder does not have its log.
		let deadline = session.server.control_read_deadline();
		let arg =
			Self::get_process_holder_arg(session, deadline, request_id, exhaustive, &mut wakeups);
		let index_required = |reply: &tg::process::control::ClientResponseOutput| {
			metadata
				|| matches!(
					reply,
					tg::process::control::ClientResponseOutput::Get(output)
						if output.data.status.is_finished()
				)
		};
		let output = self
			.request_process_holder(session, id, arg, index_required)
			.boxed()
			.await?
			.ok_or_else(|| tg::error!("the process status wakeup stream ended"))?;
		let control = output
			.reply
			.and_then(Result::ok)
			.and_then(Self::try_get_process_data_from_holder_reply);

		let (data, location, metadata) = match (control, output.indexed) {
			(None, None) => return Ok(None),
			(None, Some((indexed, indexed_data))) => {
				let Some(data) = indexed_data else {
					return Ok(None);
				};
				let metadata = metadata.then_some(indexed.metadata);
				(data, indexed.location, metadata)
			},
			(Some(data), None) => (data, None, None),
			(Some(data), Some((indexed, indexed_data))) => {
				let data = indexed_data
					.filter(|data| data.status.is_finished())
					.unwrap_or(data);
				let metadata = metadata.then_some(indexed.metadata);
				(data, indexed.location, metadata)
			},
		};
		let output = Self::create_process_get_output(session, id, data, location, metadata);

		Ok(Some(output))
	}

	/// Create the arg for a get request to the holder of a process, which the index answers when the holder does not.
	pub(super) fn get_process_holder_arg<'a>(
		session: &Session,
		deadline: tokio::time::Instant,
		id: String,
		exhaustive: bool,
		wakeups: &'a mut BoxStream<'static, ()>,
	) -> crate::holder::Arg<'a> {
		let request = tg::process::control::ServerRequestArg::Get(
			tg::process::control::GetServerRequestArg {},
		);
		let retry = session.server.config.control.get_retry.clone().into();
		let timeout = session.server.config.control.read_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		crate::holder::Arg {
			checkpoint: Some("process.get"),
			deadline,
			exhaustive,
			id,
			mode: tangram_server_core::holder::Mode::Read,
			options,
			request,
			wakeups,
		}
	}

	/// Get the process's data from its holder's reply to a get request.
	pub(super) fn try_get_process_data_from_holder_reply(
		reply: tg::process::control::ClientResponseOutput,
	) -> Option<tg::process::Data> {
		let mut output = reply.try_unwrap_get().ok()?;
		if let Some(sync) = &output.sync {
			Self::inherit_process_authorization_tokens_for_sync(&mut output.data, sync);
		}
		Some(output.data)
	}

	pub(crate) async fn get_process_from_index(
		&self,
		session: &Session,
		id: &tg::process::Id,
	) -> tg::Result<tangram_index::process::Process> {
		self.try_get_process_from_index(session, id)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the process in the index"))
	}

	pub(crate) async fn try_get_process_from_index(
		&self,
		session: &Session,
		id: &tg::process::Id,
	) -> tg::Result<Option<tangram_index::process::Process>> {
		if let Some(process) = session.server.index.try_get_process(id).await? {
			return Ok(Some(process));
		}
		session
			.server
			.components
			.indexer
			.index(session, tg::index::Arg::default())
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;
		session.server.index.try_get_process(id).await
	}

	pub(crate) async fn get_process_with_data_from_index(
		&self,
		session: &Session,
		id: &tg::process::Id,
	) -> tg::Result<(tangram_index::process::Process, Option<tg::process::Data>)> {
		self.try_get_process_with_data_from_index(session, id)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the process in the index"))
	}

	/// Read a process and its data from the index, awaiting indexing on a miss only if the read is exhaustive.
	pub(crate) async fn try_get_process_with_data_from_index_with_exhaustive(
		&self,
		session: &Session,
		id: &tg::process::Id,
		exhaustive: bool,
	) -> tg::Result<Option<(tangram_index::process::Process, Option<tg::process::Data>)>> {
		if exhaustive {
			self.try_get_process_with_data_from_index(session, id).await
		} else {
			session.server.index.try_get_process_with_data(id).await
		}
	}

	pub(crate) async fn try_get_process_with_data_from_index(
		&self,
		session: &Session,
		id: &tg::process::Id,
	) -> tg::Result<Option<(tangram_index::process::Process, Option<tg::process::Data>)>> {
		if let Some(output) = session.server.index.try_get_process_with_data(id).await? {
			return Ok(Some(output));
		}
		session
			.server
			.components
			.indexer
			.index(session, tg::index::Arg::default())
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;
		session.server.index.try_get_process_with_data(id).await
	}

	fn create_process_get_output(
		session: &Session,
		id: &tg::process::Id,
		data: tg::process::Data,
		location: Option<tg::Location>,
		metadata: Option<tg::process::Metadata>,
	) -> tg::process::get::Output {
		let location = location.unwrap_or_else(|| {
			tg::Location::Local(tg::location::Local {
				region: session.server.config.region.clone(),
			})
		});
		tg::process::get::Output {
			availability: None,
			data,
			id: id.clone(),
			location: Some(location),
			metadata,
			tokens: tg::authorization::Tokens::default(),
		}
	}

	pub async fn try_get_process_regions(
		&self,
		session: &Session,
		arg: TryGetProcessRegionsArg<'_>,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let TryGetProcessRegionsArg {
			availability,
			id,
			metadata,
			regions,
			tokens,
		} = arg;
		let mut futures = regions
			.iter()
			.map(|region| {
				let arg = TryGetProcessRegionArg {
					availability,
					id,
					metadata,
					region,
					tokens,
				};
				self.try_get_process_region(session, arg)
			})
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_get_process_region(
		&self,
		session: &Session,
		arg: TryGetProcessRegionArg<'_>,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let TryGetProcessRegionArg {
			availability,
			id,
			metadata,
			region,
			tokens,
		} = arg;
		let client = session
			.get_region_session_for_process(region)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the region client"),
			)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::get::Arg {
			availability,
			location: Some(location.clone().into()),
			metadata,
			tokens: tokens.for_location(&location),
		};
		let Some(mut output) = client.try_get_process(id, arg).await.map_err(
			|error| tg::error!(!error, %id, region = %region, "failed to get the process"),
		)?
		else {
			return Ok(None);
		};
		session
			.server
			.components
			.authorization
			.update_tokens_and_location(
				session,
				&mut output.tokens,
				Some(&mut output.location),
				&location,
				false,
			)?;
		session
			.server
			.components
			.authorization
			.update_process_data_referents_for_location(
				session,
				&mut output.data,
				&location,
				false,
			)?;
		Ok(Some(output))
	}

	pub async fn try_get_process_remotes(
		&self,
		session: &Session,
		arg: TryGetProcessRemotesArg<'_>,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let TryGetProcessRemotesArg {
			availability,
			id,
			metadata,
			remotes,
			tokens,
		} = arg;
		let mut futures = remotes
			.iter()
			.map(|remote| {
				let arg = TryGetProcessRemoteArg {
					availability,
					id,
					metadata,
					remote,
					tokens,
				};
				self.try_get_process_remote(session, arg)
			})
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};

		// Spawn a task to put the process if it is finished.
		if output.data.status.is_finished() {
			self.spawn_remote_process_put_task(
				session,
				id,
				&output.data,
				output.location.as_ref(),
				&output.tokens,
			);
		}

		Ok(Some(output))
	}

	fn spawn_remote_process_put_task(
		&self,
		session: &Session,
		id: &tg::process::Id,
		data: &tg::process::Data,
		location: Option<&tg::Location>,
		tokens: &tg::authorization::Tokens,
	) {
		let component = self.clone();
		let mut task_session = session.clone();
		task_session.context.stopper = None;
		self.remote_put_tasks
			.spawn(|_| {
				let data = data.clone();
				let id = id.clone();
				let location = location.cloned().map(Into::into);
				let tokens = tokens.clone();
				async move {
					if let Err(error) = component
						.cache_process_remote_task(&task_session, &id, data, location, tokens)
						.boxed()
						.await
					{
						tracing::error!(error = %error.trace(), %id, "failed to cache the process");
					}
				}
			})
			.detach();
	}

	async fn cache_process_remote_task(
		&self,
		session: &Session,
		id: &tg::process::Id,
		mut data: tg::process::Data,
		location: Option<tg::location::Arg>,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<()> {
		let children = if let Some(children) = data.children.take() {
			children
		} else {
			let arg = tg::process::children::get::Arg {
				location: location.clone(),
				tokens,
				..Default::default()
			};
			session
				.try_get_process_children(id, arg)
				.await?
				.ok_or_else(|| tg::error!("expected the process to exist"))?
				.map_ok(|chunk| stream::iter(chunk.data).map(Ok::<_, tg::Error>))
				.try_flatten()
				.try_collect()
				.await?
		};
		data.children = Some(children);
		let arg = tg::process::put::Arg {
			data,
			location: None,
		};
		let options = tangram_server_process_core::put::Options {
			command_sync: None,
			defer_index: false,
			location: location.and_then(|location| location.to_location()),
			outcome_sync: None,
			registration: None,
			store_data: true,
		};
		self.put_process_local(session, id, arg, options)
			.boxed()
			.await?;

		Ok(())
	}

	async fn try_get_process_remote(
		&self,
		session: &Session,
		arg: TryGetProcessRemoteArg<'_>,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let TryGetProcessRemoteArg {
			availability,
			id,
			metadata,
			remote,
			tokens,
		} = arg;
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
		let location = tg::location::Arg(vec![tg::location::arg::Component::Local(
			tg::location::arg::LocalComponent {
				regions: remote.regions.clone(),
			},
		)]);
		let arg = tg::process::get::Arg {
			availability,
			location: Some(location),
			metadata,
			tokens: tokens.for_location(&tg::Location::Remote(tg::location::Remote {
				name: remote.name.clone(),
				region: None,
			})),
		};
		let Some(mut output) = client.try_get_process(id, arg).await.map_err(
			|error| tg::error!(!error, %id, remote = %remote.name, "failed to get the process"),
		)?
		else {
			return Ok(None);
		};
		let region = match output.location.take() {
			Some(tg::Location::Local(local)) => local.region,
			Some(tg::Location::Remote(remote)) => remote.region,
			None => None,
		};
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region,
		});
		session
			.server
			.components
			.authorization
			.update_tokens_and_location(
				session,
				&mut output.tokens,
				Some(&mut output.location),
				&location,
				trusted,
			)?;
		session
			.server
			.components
			.authorization
			.update_process_data_referents_for_location(
				session,
				&mut output.data,
				&location,
				trusted,
			)?;
		Ok(Some(output))
	}

	pub(crate) async fn try_get_process_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the process id"))?;

		// Get the arg.
		let (arg, _) = request
			.arg::<tg::process::get::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Get the process.
		let location = arg.location.clone();
		let Some(mut output) = self.try_get_process(session, &id, arg).boxed().await? else {
			return Ok(http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.empty()
				.unwrap()
				.boxed_body());
		};
		if output.data.status.is_finished() && output.data.children.is_none() {
			let arg = tg::process::children::get::Arg {
				location,
				tokens: output.tokens.clone(),
				..Default::default()
			};
			if let Some(stream) = session.try_get_process_children(&id, arg).await? {
				let children = stream
					.map_ok(|chunk| stream::iter(chunk.data).map(Ok::<_, tg::Error>))
					.try_flatten()
					.try_collect()
					.await?;
				output.data.children = Some(children);
			}
		}

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		if let Some(metadata) = &output.metadata {
			response = response.header(
				tg::process::get::METADATA_HEADER,
				serde_json::to_string(metadata).unwrap(),
			);
		}
		if let Some(availability) = &output.availability {
			response = response.header(
				tg::process::get::AVAILABILITY_HEADER,
				serde_json::to_string(availability).unwrap(),
			);
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
