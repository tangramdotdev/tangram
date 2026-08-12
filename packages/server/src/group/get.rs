use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_database::prelude::*,
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
			.try_get_with_selector(&selector, arg.location.as_ref(), arg.cached, arg.ttl)
			.await?
		else {
			return Ok(None);
		};
		let tg::get::Node::Id(id) = output.referent.node else {
			unreachable!();
		};
		let Ok(id) = tg::group::Id::try_from(id) else {
			return Ok(None);
		};
		let location = output
			.location
			.unwrap_or_else(|| tg::Location::Local(tg::location::Local::default()));
		let tokens = output.referent.options.tokens;
		match location {
			tg::Location::Local(_) => self.try_get_group_local(&id, tokens).await,
			tg::Location::Remote(remote) => {
				self.try_get_group_remote(&id, arg, remote, tokens).await
			},
		}
	}

	async fn try_get_group_local(
		&self,
		id: &tg::group::Id,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::group::get::Output>> {
		let permission =
			tg::grant::Permission::Group(tg::grant::permission::group::Permission::Read);
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
		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let Some(mut group) = Self::try_get_group_with_transaction(&transaction, id).await? else {
			return Ok(None);
		};
		group.tokens = tokens;

		Ok(Some(group))
	}

	async fn try_get_group_remote(
		&self,
		id: &tg::group::Id,
		mut arg: tg::group::get::Arg,
		remote: tg::location::Remote,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::group::get::Output>> {
		let cached = arg.cached;
		let ttl = arg.ttl;
		let location = tg::Location::Remote(remote.clone());
		arg.cached = false;
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		arg.ttl = tg::remote::cache::Ttl::default();
		let request =
			crate::remote::cache::Request::GroupGet(crate::remote::cache::GroupGetRequest {
				arg: arg.clone(),
				id: id.clone(),
			});
		if let Some(crate::remote::cache::Response::GroupGet(response)) = self
			.try_get_cached_remote_response(&remote.name, &request, ttl)
			.await?
		{
			let mut output = response.output;
			let valid = output.as_ref().is_none_or(|group| {
				crate::remote::cache::token_valid(group.tokens.local(), &self.server.clock)
			});
			if valid || cached {
				if let Some(group) = &mut output {
					if !crate::remote::cache::token_valid(group.tokens.local(), &self.server.clock)
					{
						group.tokens.remove_local();
					}
					self.update_tokens_for_location(&mut group.tokens, &location)?;
					group.tokens.inherit(&tokens);
					group.location = Some(location);
				}

				return Ok(output);
			}
		}
		if cached {
			return Ok(None);
		}
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let group = tg::group::Selector::Id(id.clone());
		let mut output = client.try_get_group(&group, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the group"),
		)?;
		let response =
			crate::remote::cache::Response::GroupGet(crate::remote::cache::GroupGetResponse {
				output: output.clone(),
			});
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await?;
		if let Some(group) = &mut output {
			self.update_tokens_for_location(&mut group.tokens, &location)?;
			group.tokens.inherit(&tokens);
			group.location = Some(location);
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
