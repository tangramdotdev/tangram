use {
	crate::Session,
	futures::FutureExt as _,
	tangram_client::prelude::*,
	tangram_database as db,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_get_group(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::get::Arg,
	) -> tg::Result<Option<tg::group::get::Output>> {
		let selector = match group {
			tg::Selector::Id(id) => tg::Selector::Id(id.clone().into()),
			tg::Selector::Specifier(specifier) => tg::Selector::Specifier(specifier.clone()),
		};
		let Some(output) = self
			.try_get_with_selector(
				&selector,
				arg.location.as_ref(),
				&arg.tokens,
				arg.cached,
				arg.ttl,
			)
			.await?
		else {
			return Ok(None);
		};
		let Some(location) = output.referent.options.location.clone() else {
			return Err(tg::error!("expected a location"));
		};
		let tg::get::Node::Id(id) = output.referent.node else {
			unreachable!();
		};
		let Ok(id) = tg::group::Id::try_from(id) else {
			return Ok(None);
		};
		let tokens = output.referent.options.tokens;
		match location {
			tg::Location::Local(_) => self.try_get_group_local(&id, tokens).await,
			tg::Location::Remote(remote) => {
				self.try_get_group_remote(&id, arg, remote, tokens).await
			},
		}
	}

	pub(crate) async fn try_get_group_local(
		&self,
		id: &tg::group::Id,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::group::get::Output>> {
		let permission = tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Read,
		);
		let authorized = self
			.authorize(
				tg::Referent::with_node_and_tokens(
					tg::group::Selector::Id(id.clone()),
					tokens.clone(),
				),
				permission,
			)
			.await?;
		if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
			return Ok(None);
		}
		let id = id.clone();
		let group = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let id = id.clone();
				async move { Self::try_get_group_with_transaction(transaction, &id).await }.boxed()
			})
			.await?;
		let Some(data) = group else {
			return Ok(None);
		};
		let output = tg::group::get::Output {
			data,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			tokens,
		};

		Ok(Some(output))
	}

	async fn try_get_group_remote(
		&self,
		id: &tg::group::Id,
		mut arg: tg::group::get::Arg,
		remote: tg::location::Remote,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::group::get::Output>> {
		let cached = arg.cached;
		let cacheable = arg.tokens.is_empty();
		let ttl = arg.ttl;
		let location = tg::Location::Remote(remote.clone());
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let trusted = client.trusted();
		arg.cached = false;
		arg.tokens = arg.tokens.for_location(&location);
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		arg.ttl = tg::remote::cache::Ttl::default();
		let mut cache_arg = arg.clone();
		cache_arg.tokens.clear();
		let request =
			crate::remote::cache::Request::GroupGet(crate::remote::cache::GroupGetRequest {
				arg: cache_arg,
				id: id.clone(),
			});
		if let Some(crate::remote::cache::Response::GroupGet(response)) = self
			.try_get_cached_remote_response(&remote.name, &request, ttl)
			.await?
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
					self.update_tokens_and_location(
						&mut output.tokens,
						Some(&mut output.location),
						&location,
						trusted,
					)?;
					output.tokens.inherit(&tokens);
				}

				return Ok(output);
			}
		}
		if cached {
			return Ok(None);
		}
		let group = tg::group::Selector::Id(id.clone());
		let mut output = client.try_get_group(&group, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the group"),
		)?;
		let response =
			crate::remote::cache::Response::GroupGet(crate::remote::cache::GroupGetResponse {
				output: output.clone(),
			});
		if cacheable {
			self.put_cached_remote_response(&remote.name, &request, &response)
				.await?;
		}
		if let Some(output) = &mut output {
			self.update_tokens_and_location(
				&mut output.tokens,
				Some(&mut output.location),
				&location,
				trusted,
			)?;
			output.tokens.inherit(&tokens);
		}

		Ok(output)
	}

	pub(crate) async fn try_get_group_request(
		&self,
		request: http::Request<BoxBody>,
		group: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let group = group.replace(':', "/").parse()?;
		let Some(output) = self.try_get_group(&group, arg).await? else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
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
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}
}
