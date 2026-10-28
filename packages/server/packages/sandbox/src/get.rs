use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
	tangram_server_core::Session,
};

pub(super) struct Output<T> {
	pub control: Option<T>,
	pub indexed: Option<tangram_index::sandbox::Sandbox>,
}

pub(super) struct GetSandboxStateLocalArg<'a, F, R> {
	pub control_future: F,
	pub deadline: tokio::time::Instant,
	pub id: &'a tg::sandbox::Id,
	pub index_required: R,
	pub processes: bool,
	pub source: tg::sandbox::Source,
}

struct TryGetSandboxRemotesArg<'a> {
	cached: bool,
	id: &'a tg::sandbox::Id,
	remotes: &'a [tangram_server_core::location::Remote],
	source: tg::sandbox::Source,
	tokens: &'a tg::authorization::Tokens,
	ttl: tg::remote::cache::Ttl,
}

struct TryGetSandboxRemoteArg<'a> {
	cached: bool,
	id: &'a tg::sandbox::Id,
	remote: &'a tangram_server_core::location::Remote,
	source: tg::sandbox::Source,
	tokens: &'a tg::authorization::Tokens,
	ttl: tg::remote::cache::Ttl,
}

impl Component {
	pub(crate) async fn try_get_sandbox(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let runner = if arg.source.is_index() {
			None
		} else {
			self.try_get_sandbox_runner(session, id, &arg)
				.boxed()
				.await?
		};
		if let Some(output) = &runner
			&& (arg.source.is_runner() || !output.data.status.is_destroyed())
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

		if locations.local.as_ref().is_some_and(|local| local.current)
			&& let Some(output) = self
				.try_get_sandbox_local(session, id, arg.tokens.local_authorization(), arg.source)
				.await?
		{
			let output = if output.data.status.is_destroyed() {
				output
			} else {
				runner.unwrap_or(output)
			};
			return Ok(Some(output));
		}

		if let Some(local) = &locations.local
			&& let Some(output) = self
				.try_get_sandbox_regions(session, id, &local.regions, &arg.tokens, arg.source)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the sandbox from another region"),
				)? {
			let output = if output.data.status.is_destroyed() {
				output
			} else {
				runner.unwrap_or(output)
			};
			return Ok(Some(output));
		}

		let remotes_arg = TryGetSandboxRemotesArg {
			cached: arg.cached,
			id,
			remotes: &locations.remotes,
			source: arg.source,
			tokens: &arg.tokens,
			ttl: arg.ttl,
		};
		if let Some(output) = self
			.try_get_sandbox_remotes(session, remotes_arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox from a remote"))?
		{
			let output = if output.data.status.is_destroyed() {
				output
			} else {
				runner.unwrap_or(output)
			};
			return Ok(Some(output));
		}

		Ok(runner)
	}

	async fn try_get_sandbox_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::get::Arg,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let Some(runner) = session
			.server
			.components
			.runner
			.try_get_sandbox_runner(id, arg.location.as_ref())
		else {
			return Ok(None);
		};
		if !self
			.authorize_sandbox_runner(
				session,
				id,
				arg.tokens.local_authorization(),
				tg::authorization::permission::sandbox::Permission::Node,
			)
			.await?
		{
			return Ok(None);
		}
		let Some(mut output) = session
			.server
			.components
			.runner
			.state()
			.sandboxes()
			.get(runner.index)
			.map(|sandbox| sandbox.data())
		else {
			return Ok(None);
		};

		// The runner's capabilities belong to the runner, not to the caller.
		output.tokens.clear();
		if let Some(token) = session
			.server
			.components
			.authorization
			.create_read_token(session, &id.clone().into())?
		{
			output.tokens.insert_local_authorization(token);
		}
		Ok(Some(output))
	}

	pub(crate) async fn try_get_sandbox_local(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		tokens: &[tg::authorization::Token],
		source: tg::sandbox::Source,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Node,
		);
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let authorize_future = session.authorize(resource, permission).boxed();
		let get_future = self
			.try_get_sandbox_local_inner(session, id, source)
			.boxed();
		let (permissions, output) = future::try_join(authorize_future, get_future).await?;
		let permissions = permissions.check_exhaustion()?;
		if !permissions.permissions.contains(permission) {
			return Ok(None);
		}
		let Some(mut output) = output else {
			return Ok(None);
		};
		output.tokens.clear();
		if let Some(token) = session
			.server
			.components
			.authorization
			.create_read_token(session, &id.clone().into())?
		{
			output.tokens.insert_local_authorization(token);
		}
		Ok(Some(output))
	}

	pub(crate) async fn try_get_sandbox_local_inner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		source: tg::sandbox::Source,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		// Subscribe before reading to avoid missing a status change between the read and subscription.
		let mut wakeups = self
			.create_sandbox_status_wakeup_stream(session, id, None, None)
			.await?;
		let deadline = session.server.control_read_deadline();
		let output = loop {
			let arg = GetSandboxStateLocalArg {
				control_future: self.get_sandbox_from_control(session, id),
				deadline,
				id,
				index_required: |data: &tg::sandbox::get::Output| data.data.status.is_destroyed(),
				processes: false,
				source,
			};
			tokio::select! {
				output = self.get_sandbox_state_local(session, arg).boxed() => break output?,
				wakeup = wakeups.next() => {
					if wakeup.is_none() {
						return Err(tg::error!("the sandbox status wakeup stream ended"));
					}
				},
			}
		};

		let output = match (output.control, output.indexed) {
			(None, None) => return Ok(None),
			(None, Some(indexed)) => {
				let Some(data) = indexed.data else {
					return Ok(None);
				};
				data
			},
			(Some(data), None) => data,
			(Some(data), Some(indexed)) => indexed
				.data
				.filter(|data| data.data.status.is_destroyed())
				.unwrap_or(data),
		};

		Ok(Some(output))
	}

	pub(super) async fn get_sandbox_state_local<T, F, R>(
		&self,
		session: &Session,
		arg: GetSandboxStateLocalArg<'_, F, R>,
	) -> tg::Result<Output<T>>
	where
		T: Send,
		F: Future<Output = tg::Result<T>> + Send,
		R: Fn(&T) -> bool + Send,
	{
		let GetSandboxStateLocalArg {
			control_future,
			deadline,
			id,
			index_required,
			processes,
			source,
		} = arg;
		let index_complete = |sandbox: &tangram_index::sandbox::Sandbox| {
			processes
				|| sandbox
					.data
					.as_ref()
					.is_some_and(|data| data.data.status.is_destroyed())
		};
		let get_index = || async {
			let indexed = self.try_get_sandbox_from_index(session, id).await?;
			Ok::<_, tg::Error>(indexed.filter(|sandbox| !processes || sandbox.set.processes))
		};
		let index_future = async {
			let indexed = get_index().await;
			tangram_server_core::checkpoint!(session.server, "sandbox.get.index", sandbox = %id)
				.await;
			indexed
		}
		.boxed();
		let control_future = session
			.server
			.read_control_response_until(deadline, async {
				let output = control_future.await;
				tangram_server_core::checkpoint!(session.server, "sandbox.get.control", sandbox = %id).await;
				output
			})
			.boxed();

		match source {
			tg::sandbox::Source::Auto => {},
			tg::sandbox::Source::Index => {
				let indexed = index_future.await?;
				return Ok(Output {
					control: None,
					indexed,
				});
			},
			tg::sandbox::Source::Runner => {
				let control = control_future.await.ok();
				return Ok(Output {
					control,
					indexed: None,
				});
			},
		}

		let (control, indexed) = match future::select(index_future, control_future).await {
			future::Either::Left((indexed, control_future)) => {
				let indexed = indexed?;
				if indexed.as_ref().is_some_and(|sandbox| {
					index_complete(sandbox)
						|| sandbox
							.location
							.as_ref()
							.is_some_and(tg::Location::is_remote)
				}) {
					let output = Output {
						control: None,
						indexed,
					};
					return Ok(output);
				}

				// Report the sandbox as not found when the index has no record of it after indexing everything enqueued before the lookup, because a sandbox's index insertion is enqueued before anyone other than its runner can learn its ID.
				if indexed.is_none()
					&& (!processes || session.server.index.try_get_sandbox(id).await?.is_none())
				{
					let output = Output {
						control: None,
						indexed: None,
					};
					return Ok(output);
				}

				let control = control_future.await;
				let indexed = if control.as_ref().is_ok_and(&index_required) {
					get_index().await?
				} else {
					indexed
				};
				(control, indexed)
			},
			future::Either::Right((control, index_future)) => {
				let indexed = if control.as_ref().is_ok_and(&index_required) {
					let indexed = index_future.await?;
					if indexed.as_ref().is_some_and(index_complete) {
						indexed
					} else {
						get_index().await?
					}
				} else {
					None
				};
				(control, indexed)
			},
		};

		let control = match control {
			Err(_) => {
				let indexed = get_index().await?;
				let output = Output {
					control: None,
					indexed,
				};
				return Ok(output);
			},
			Ok(control) => Some(control),
		};
		let output = Output { control, indexed };

		Ok(output)
	}

	pub async fn get_sandbox_from_index(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
	) -> tg::Result<tangram_index::sandbox::Sandbox> {
		self.try_get_sandbox_from_index(session, id)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox in the index"))
	}

	pub async fn try_get_sandbox_from_index(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tangram_index::sandbox::Sandbox>> {
		if let Some(sandbox) = session.server.index.try_get_sandbox(id).await? {
			return Ok(Some(sandbox));
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
		session.server.index.try_get_sandbox(id).await
	}

	pub(crate) async fn get_sandbox_from_control(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
	) -> tg::Result<tg::sandbox::get::Output> {
		let request = tg::sandbox::control::ServerRequestArg::Get(
			tg::sandbox::control::GetServerRequestArg {},
		);
		let retry = session.server.config.control.get_retry.clone().into();
		let timeout = session.server.config.control.read_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		let response = self
			.request_sandbox_control(session, id, request, options)
			.boxed()
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to send the get sandbox control request"),
			)?
			.map_err(|error| tg::error!(!error, %id, "the get sandbox control request failed"))?;
		let response = response
			.try_unwrap_get()
			.map_err(|_| tg::error!("expected a get response"))?;
		let mut output = response.data;
		output.location = Some(tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		}));
		Ok(output)
	}

	async fn try_get_sandbox_regions(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		regions: &[String],
		tokens: &tg::authorization::Tokens,
		source: tg::sandbox::Source,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_sandbox_region(session, id, region, tokens, source))
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

	async fn try_get_sandbox_region(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		region: &str,
		tokens: &tg::authorization::Tokens,
		source: tg::sandbox::Source,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let client = session.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::get::Arg {
			location: Some(location.clone().into()),
			source,
			tokens: tokens.for_location(&location),
			..tg::sandbox::get::Arg::default()
		};
		let Some(mut output) = client.try_get_sandbox(id, arg).await.map_err(
			|error| tg::error!(!error, %id, region = %region, "failed to get the sandbox"),
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
		Ok(Some(output))
	}

	async fn try_get_sandbox_remotes(
		&self,
		session: &Session,
		arg: TryGetSandboxRemotesArg<'_>,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let TryGetSandboxRemotesArg {
			cached,
			id,
			remotes,
			source,
			tokens,
			ttl,
		} = arg;
		let mut futures = remotes
			.iter()
			.map(|remote| {
				let arg = TryGetSandboxRemoteArg {
					cached,
					id,
					remote,
					source,
					tokens,
					ttl,
				};
				self.try_get_sandbox_remote(session, arg)
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
				Err(source) => result = Err(source),
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_get_sandbox_remote(
		&self,
		session: &Session,
		arg: TryGetSandboxRemoteArg<'_>,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let TryGetSandboxRemoteArg {
			cached,
			id,
			remote,
			source,
			tokens,
			ttl,
		} = arg;
		// Create the remote request.
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let arg = tg::sandbox::get::Arg {
			cached: false,
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			source,
			tokens: tokens.for_location(&location),
			ttl: tg::remote::cache::Ttl::default(),
		};
		let request = tangram_server_remote_core::cache::Request::SandboxGet(
			tangram_server_remote_core::cache::SandboxGetRequest {
				arg: arg.clone(),
				id: id.clone(),
			},
		);
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, %id, remote = %remote.name, "failed to get the remote client"),
			)?;
		let trusted = client.trusted();

		// Get a cached response.
		if source.is_auto()
			&& let Some(tangram_server_remote_core::cache::Response::SandboxGet(response)) = session
				.server
				.components
				.remote
				.try_get_cached_remote_response(session, &remote.name, &request, ttl)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
			&& let Some(mut output) = response.output
		{
			let valid = tangram_server_remote_common::tokens_valid(
				output.tokens.local_authorization(),
				&session.server.clock,
			);
			if valid || cached {
				tangram_server_remote_common::remove_expired_tokens(
					&mut output.tokens,
					&session.server.clock,
				);
				Self::set_remote_sandbox_location(session, &mut output, remote, trusted)?;

				return Ok(Some(output));
			}
		}
		if cached && source.is_auto() {
			return Ok(None);
		}

		// Get the sandbox from the remote.
		let mut output = client.try_get_sandbox(id, arg).await.map_err(
			|error| tg::error!(!error, %id, remote = %remote.name, "failed to get the sandbox"),
		)?;
		if output
			.as_ref()
			.is_some_and(|output| output.data.status.is_destroyed())
		{
			let response = tangram_server_remote_core::cache::Response::SandboxGet(
				tangram_server_remote_core::cache::SandboxGetResponse {
					output: output.clone(),
				},
			);
			session
				.server
				.components
				.remote
				.put_cached_remote_response(session, &remote.name, &request, &response)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		}
		if let Some(output) = &mut output {
			Self::set_remote_sandbox_location(session, output, remote, trusted)?;
		}

		Ok(output)
	}

	fn set_remote_sandbox_location(
		session: &Session,
		output: &mut tg::sandbox::get::Output,
		remote: &tangram_server_core::location::Remote,
		trusted: bool,
	) -> tg::Result<()> {
		let region = match output.location.as_ref() {
			Some(tg::Location::Local(local)) => local.region.clone(),
			_ => None,
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
		Ok(())
	}

	pub(crate) async fn try_get_sandbox_request(
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

		// Parse the sandbox id.
		let id = id
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the sandbox id"))?;

		// Get the arg.
		let (arg, _) = request
			.arg::<tg::sandbox::get::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Get the sandbox.
		let Some(output) = self.try_get_sandbox(session, &id, arg).boxed().await? else {
			return Ok(http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.empty()
				.unwrap()
				.boxed_body());
		};

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
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
