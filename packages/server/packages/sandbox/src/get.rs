use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
	tangram_server_core::{Session, location::await_local_future_over_regions_remotes_futures},
};

struct TryGetSandboxRemotesArg<'a> {
	cached: bool,
	id: &'a tg::sandbox::Id,
	remotes: &'a [tangram_server_core::location::Remote],
	tokens: &'a tg::authorization::Tokens,
	ttl: tg::remote::cache::Ttl,
}

struct TryGetSandboxRemoteArg<'a> {
	cached: bool,
	id: &'a tg::sandbox::Id,
	remote: &'a tangram_server_core::location::Remote,
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
		let runner = self
			.try_get_sandbox_runner(session, id, &arg)
			.boxed()
			.await?;
		if let Some(output) = &runner
			&& !output.data.status.is_destroyed()
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

		// Read the sandbox and authorize it without a search.
		let node = if current {
			self.try_get_sandbox_local_inner(session, id, false)
				.boxed()
				.await?
		} else {
			None
		};
		if current
			&& let Some(output) = self
				.try_get_sandbox_local(
					session,
					id,
					arg.tokens.local_authorization(),
					node.clone(),
					false,
				)
				.await?
		{
			let output = if output.data.status.is_stopped_or_destroyed() {
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
			self.try_get_sandbox_local(session, id, arg.tokens.local_authorization(), node, true)
				.await
		};
		let regions_and_remotes_future = async {
			if let Some(local) = &locations.local
				&& let Some(output) = self
					.try_get_sandbox_regions(session, id, &local.regions, &arg.tokens)
					.await
					.map_err(
						|error| tg::error!(!error, %id, "failed to get the sandbox from another region"),
					)? {
				return Ok(Some(output));
			}
			let remotes_arg = TryGetSandboxRemotesArg {
				cached: arg.cached,
				id,
				remotes: &locations.remotes,
				tokens: &arg.tokens,
				ttl: arg.ttl,
			};
			self.try_get_sandbox_remotes(session, remotes_arg)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox from a remote"))
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
		let output = if output.data.status.is_stopped_or_destroyed() {
			output
		} else {
			runner.unwrap_or(output)
		};

		Ok(Some(output))
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
		node: Option<tg::sandbox::get::Output>,
		exhaustive: bool,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Node,
		);
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let permissions = session
			.authorize_with_exhaustive(resource, permission, exhaustive)
			.boxed()
			.await?
			.check_exhaustion()?;
		if !permissions.permissions.contains(permission) {
			return Ok(None);
		}
		let output = match node {
			Some(node) => Some(node),
			None => {
				self.try_get_sandbox_local_inner(session, id, exhaustive)
					.boxed()
					.await?
			},
		};
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
		exhaustive: bool,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		// Subscribe before reading to avoid missing a status change between the read and subscription.
		let mut wakeups = self
			.create_sandbox_status_wakeup_stream(session, id, None, None)
			.await?;

		// Get the sandbox from its holder or the index. A destroyed sandbox requires the index.
		let deadline = session.server.control_read_deadline();
		let arg = Self::get_sandbox_holder_arg(session, deadline, exhaustive);
		let arg = crate::holder::Arg {
			wakeups: Some(&mut wakeups),
			..arg
		};
		let index_required = |reply: &tg::sandbox::control::ClientResponseOutput| {
			matches!(
				reply,
				tg::sandbox::control::ClientResponseOutput::Get(output)
					if output.data.data.status.is_destroyed()
			)
		};
		let output = self
			.request_sandbox_holder(session, id, arg, index_required)
			.boxed()
			.await?
			.ok_or_else(|| tg::error!("the sandbox status wakeup stream ended"))?;
		let control = output
			.reply
			.and_then(Result::ok)
			.and_then(|reply| Self::try_get_sandbox_from_holder_reply(session, reply));

		let output = match (control, output.indexed) {
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

	/// Create the arg for a get request to the holder of a sandbox, which the index answers when the holder does not.
	pub(super) fn get_sandbox_holder_arg(
		session: &Session,
		deadline: tokio::time::Instant,
		exhaustive: bool,
	) -> crate::holder::Arg<'static> {
		let request = tg::sandbox::control::ServerRequestArg::Get(
			tg::sandbox::control::GetServerRequestArg {},
		);
		let retry = session.server.config.control.get_retry.clone().into();
		let timeout = session.server.config.control.read_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		crate::holder::Arg {
			deadline,
			exhaustive,
			fallback: true,
			options,
			request,
			wakeups: None,
		}
	}

	/// Get the sandbox from its holder's reply to a get request.
	pub(super) fn try_get_sandbox_from_holder_reply(
		session: &Session,
		reply: tg::sandbox::control::ClientResponseOutput,
	) -> Option<tg::sandbox::get::Output> {
		let mut output = reply.try_unwrap_get().ok()?.data;
		output.location = Some(tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		}));
		Some(output)
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

	/// Read a sandbox from the index, awaiting indexing on a miss only if the read is exhaustive.
	pub(crate) async fn try_get_sandbox_from_index_with_exhaustive(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		exhaustive: bool,
	) -> tg::Result<Option<tangram_index::sandbox::Sandbox>> {
		if exhaustive {
			self.try_get_sandbox_from_index(session, id).await
		} else {
			session.server.index.try_get_sandbox(id).await
		}
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

	async fn try_get_sandbox_regions(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		regions: &[String],
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_sandbox_region(session, id, region, tokens))
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
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let client = session.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::get::Arg {
			location: Some(location.clone().into()),
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
		if let Some(tangram_server_remote_core::cache::Response::SandboxGet(response)) = session
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
		if cached {
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
