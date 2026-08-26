use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_get_sandbox_local(id)
					.boxed()
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_get_sandbox_regions(id, &local.regions)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the sandbox from another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_get_sandbox_remotes(id, &locations.remotes, arg.cached, arg.ttl)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox from a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Read,
		);
		let authorize_future = async {
			let authorized = self.authorize(id.clone(), permission).await?;
			Ok::<_, tg::Error>(
				authorized.is_some_and(|permissions| permissions.contains(permission)),
			)
		}
		.boxed();
		let get_future = self.try_get_sandbox_local_inner(id).boxed();
		let (authorized, mut output) = future::try_join(authorize_future, get_future).await?;
		if !authorized {
			return Ok(None);
		}
		if let Some(output) = &mut output
			&& let Some(token) = self.create_read_token(&id.clone().into())?
		{
			output.tokens.set_local(token);
		}
		Ok(output)
	}

	pub(crate) async fn try_get_sandbox_local_inner(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		if let Some(data) = self.server.runner.state().try_get_sandbox(id)
			&& !data.status.is_destroyed()
		{
			return Ok(Some(data));
		}

		let index_future = self.try_get_sandbox_from_index(id).boxed();
		let control_future = self.get_sandbox_from_control(id).boxed();
		let output = match future::select(index_future, control_future).await {
			future::Either::Left((indexed, control_future)) => {
				let Some(indexed) = indexed? else {
					return Ok(None);
				};
				if indexed
					.data
					.as_ref()
					.is_some_and(|data| data.status.is_destroyed())
				{
					indexed.data.unwrap()
				} else {
					let data = control_future.await?;
					if data.status.is_destroyed() {
						let Some(indexed) = self.try_get_sandbox_from_index(id).await? else {
							return Ok(None);
						};
						indexed
							.data
							.ok_or_else(|| tg::error!(%id, "missing the sandbox data"))?
					} else {
						data
					}
				}
			},
			future::Either::Right((data, _)) => {
				let data = data?;
				if data.status.is_destroyed() {
					let Some(indexed) = self.try_get_sandbox_from_index(id).await? else {
						return Ok(None);
					};
					indexed
						.data
						.ok_or_else(|| tg::error!(%id, "missing the sandbox data"))?
				} else {
					data
				}
			},
		};
		Ok(Some(output))
	}

	pub(crate) async fn get_sandbox_from_index(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<tangram_index::sandbox::Sandbox> {
		self.try_get_sandbox_from_index(id)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox in the index"))
	}

	pub(crate) async fn try_get_sandbox_from_index(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tangram_index::sandbox::Sandbox>> {
		if let Some(sandbox) = self.server.index.try_get_sandbox(id).await? {
			return Ok(Some(sandbox));
		}
		self.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;
		self.server.index.try_get_sandbox(id).await
	}

	pub(crate) async fn get_sandbox_from_control(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<tg::sandbox::get::Output> {
		let request = tg::sandbox::control::ServerRequestArg::Get(
			tg::sandbox::control::GetServerRequestArg {},
		);
		let retry = tangram_futures::retry::Options {
			max_retries: u64::MAX,
			..Default::default()
		};
		let options = crate::control::Options {
			retry,
			timeout: std::time::Duration::from_secs(10),
		};
		let response = self
			.send_sandbox_control_request(id, request, options)
			.boxed()
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to send the get sandbox control request"),
			)?
			.map_err(|error| tg::error!(!error, %id, "the get sandbox control request failed"))?;
		let response = response
			.try_unwrap_get()
			.map_err(|_| tg::error!("expected a get response"))?;
		let output = response.data;
		Ok(output)
	}

	async fn try_get_sandbox_regions(
		&self,
		id: &tg::sandbox::Id,
		regions: &[String],
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_sandbox_region(id, region))
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
		id: &tg::sandbox::Id,
		region: &str,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::get::Arg {
			location: Some(location.clone().into()),
			..tg::sandbox::get::Arg::default()
		};
		let Some(mut output) = client
			.try_get_sandbox(id, arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to get the sandbox"))?
		else {
			return Ok(None);
		};
		self.update_tokens_and_location(
			&mut output.tokens,
			Some(&mut output.location),
			&location,
			false,
		)?;
		Ok(Some(output))
	}

	async fn try_get_sandbox_remotes(
		&self,
		id: &tg::sandbox::Id,
		remotes: &[crate::location::Remote],
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let results = remotes
			.iter()
			.map(|remote| async move {
				let name = remote.name.clone();
				let result = self.try_get_sandbox_remote(id, remote, cached, ttl).await;
				(name, result)
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;
		let mut results = results;
		results.sort_by(|a, b| a.0.cmp(&b.0));
		let mut output = None;
		for (name, result) in results {
			let result = result
				.map_err(|error| tg::error!(!error, remote = %name, "failed to get the sandbox"))?;
			if output.is_none() {
				output = result;
			}
		}

		Ok(output)
	}

	async fn try_get_sandbox_remote(
		&self,
		id: &tg::sandbox::Id,
		remote: &crate::location::Remote,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		// Create the remote request.
		let arg = tg::sandbox::get::Arg {
			cached: false,
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			ttl: tg::remote::cache::Ttl::default(),
		};
		let request =
			crate::remote::cache::Request::SandboxGet(crate::remote::cache::SandboxGetRequest {
				arg: arg.clone(),
				id: id.clone(),
			});
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, %id, remote = %remote.name, "failed to get the remote client"),
		)?;
		let trusted = client.trusted();

		// Get a cached response.
		if let Some(crate::remote::cache::Response::SandboxGet(response)) = self
			.try_get_cached_remote_response(&remote.name, &request, ttl)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
		{
			let mut output = response.output;
			let valid = output.as_ref().is_none_or(|output| {
				crate::remote::cache::token_valid(output.tokens.local(), &self.server.clock)
			});
			if valid || cached {
				if let Some(output) = &mut output {
					if !crate::remote::cache::token_valid(output.tokens.local(), &self.server.clock)
					{
						output.tokens.remove_local();
					}
					self.set_remote_sandbox_location(output, remote, trusted)?;
				}

				return Ok(output);
			}
		}
		if cached {
			return Ok(None);
		}

		// Get the sandbox from the remote.
		let mut output = client.try_get_sandbox(id, arg).await.map_err(
			|error| tg::error!(!error, %id, remote = %remote.name, "failed to get the sandbox"),
		)?;
		let response =
			crate::remote::cache::Response::SandboxGet(crate::remote::cache::SandboxGetResponse {
				output: output.clone(),
			});
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		if let Some(output) = &mut output {
			self.set_remote_sandbox_location(output, remote, trusted)?;
		}

		Ok(output)
	}

	fn set_remote_sandbox_location(
		&self,
		output: &mut tg::sandbox::get::Output,
		remote: &crate::location::Remote,
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
		self.update_tokens_and_location(
			&mut output.tokens,
			Some(&mut output.location),
			&location,
			trusted,
		)?;
		Ok(())
	}

	pub(crate) async fn try_get_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let id = id
			.parse::<tg::sandbox::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the sandbox id"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let Some(output) = self.try_get_sandbox(&id, arg).boxed().await? else {
			return Ok(http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.empty()
				.unwrap()
				.boxed_body());
		};

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
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		Ok(response.body(body).unwrap())
	}
}
