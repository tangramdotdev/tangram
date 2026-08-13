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
	pub(crate) async fn try_get_organization(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::get::Arg,
	) -> tg::Result<Option<tg::organization::get::Output>> {
		let selector = match organization {
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
		let Ok(id) = tg::organization::Id::try_from(id) else {
			return Ok(None);
		};
		let location = output
			.location
			.unwrap_or_else(|| tg::Location::Local(tg::location::Local::default()));
		let tokens = output.referent.options.tokens;
		match location {
			tg::Location::Local(_) => self.try_get_organization_local(&id, tokens).await,
			tg::Location::Remote(remote) => {
				self.try_get_organization_remote(&id, arg, remote, tokens)
					.await
			},
		}
	}

	async fn try_get_organization_local(
		&self,
		id: &tg::organization::Id,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::organization::get::Output>> {
		let permission = tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Read,
		);
		let authorized = self
			.authorize(
				tg::Referent::with_node_and_tokens(
					tg::organization::Selector::Id(id.clone()),
					tokens.clone(),
				),
				permission,
			)
			.await?;
		if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
			return Ok(None);
		}
		let id = id.clone();
		let organization = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let id = id.clone();
				async move { Self::try_get_organization_with_transaction(transaction, &id).await }
					.boxed()
			})
			.await?;
		let Some(mut organization) = organization else {
			return Ok(None);
		};
		organization.tokens = tokens;

		Ok(Some(organization))
	}

	async fn try_get_organization_remote(
		&self,
		id: &tg::organization::Id,
		mut arg: tg::organization::get::Arg,
		remote: tg::location::Remote,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::organization::get::Output>> {
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
		let request = crate::remote::cache::Request::OrganizationGet(
			crate::remote::cache::OrganizationGetRequest {
				arg: arg.clone(),
				id: id.clone(),
			},
		);
		if let Some(crate::remote::cache::Response::OrganizationGet(response)) = self
			.try_get_cached_remote_response(&remote.name, &request, ttl)
			.await?
		{
			let mut output = response.output;
			let valid = output.as_ref().is_none_or(|organization| {
				crate::remote::cache::token_valid(organization.tokens.local(), &self.server.clock)
			});
			if valid || cached {
				if let Some(organization) = &mut output {
					if !crate::remote::cache::token_valid(
						organization.tokens.local(),
						&self.server.clock,
					) {
						organization.tokens.remove_local();
					}
					self.update_tokens_for_location(&mut organization.tokens, &location)?;
					organization.tokens.inherit(&tokens);
					organization.location = Some(location);
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
		let organization = tg::organization::Selector::Id(id.clone());
		let mut output = client
			.try_get_organization(&organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the organization"),
			)?;
		let response = crate::remote::cache::Response::OrganizationGet(
			crate::remote::cache::OrganizationGetResponse {
				output: output.clone(),
			},
		);
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await?;
		if let Some(organization) = &mut output {
			self.update_tokens_for_location(&mut organization.tokens, &location)?;
			organization.tokens.inherit(&tokens);
			organization.location = Some(location);
		}

		Ok(output)
	}

	pub(crate) async fn try_get_organization_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
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
		let organization = organization.replace(':', "/").parse()?;
		let Some(output) = self.try_get_organization(&organization, arg).await? else {
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
