use {
	crate::Session,
	futures::FutureExt as _,
	tangram_client::prelude::*,
	tangram_database as db,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn try_get_tag(
		&self,
		tag: &tg::tag::Selector,
		arg: tg::tag::get::Arg,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		self.verify_request_with_network_access()?;
		let selector = match tag {
			tg::Selector::Id(id) => tg::Selector::Id(id.clone().into()),
			tg::Selector::Specifier(specifier) => tg::Selector::Specifier(specifier.clone()),
		};
		let Some(output) = self
			.try_get_with_selector(
				&selector,
				arg.location.as_ref(),
				&tg::authorization::Tokens::default(),
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
		let Ok(id) = tg::tag::Id::try_from(id) else {
			return Ok(None);
		};
		let tokens = output.referent.options.tokens;
		match location {
			tg::Location::Local(_) => self.try_get_tag_local(&id, tokens).await,
			tg::Location::Remote(remote) => self.try_get_tag_remote(&id, arg, remote, tokens).await,
		}
	}

	pub(crate) async fn try_get_tag_local(
		&self,
		id: &tg::tag::Id,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		// Get the tag.
		let id = id.clone();
		let data = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let id = id.clone();
				async move { Self::try_get_tag_data_with_transaction(transaction, &id).await }
					.boxed()
			})
			.await?;
		let Some(data) = data else {
			return Ok(None);
		};
		crate::checkpoint!(self.server, "tag.get.read", id = %id).await;

		// Authorize the tag.
		let id = id.clone().into();
		let visible = self
			.server
			.index
			.visible(std::slice::from_ref(&id), &self.context.principal)
			.await?
			.pop()
			.unwrap() || self
			.authorize(
				tg::Referent::with_node_and_tokens(tg::Selector::Id(id.clone()), tokens.clone()),
				tg::authorization::Permission::Tag(
					tg::authorization::permission::tag::Permission::Read,
				),
			)
			.await?
			.is_some_and(|permissions| {
				permissions.contains(tg::authorization::Permission::Tag(
					tg::authorization::permission::tag::Permission::Read,
				))
			});
		if !visible {
			return Ok(None);
		}

		Ok(Some(tg::tag::get::Output {
			data,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			tokens,
		}))
	}

	async fn try_get_tag_remote(
		&self,
		id: &tg::tag::Id,
		mut arg: tg::tag::get::Arg,
		remote: tg::location::Remote,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::tag::get::Output>> {
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
		let request = crate::remote::cache::Request::TagGet(crate::remote::cache::TagGetRequest {
			arg: arg.clone(),
			id: id.clone(),
		});
		if let Some(crate::remote::cache::Response::TagGet(response)) = self
			.try_get_cached_remote_response(&remote.name, &request, ttl)
			.await?
		{
			let mut output = response.output;
			let valid = output.as_ref().is_none_or(|tag| {
				crate::remote::cache::token_valid(tag.tokens.local(), &self.server.clock)
			});
			if valid || cached {
				if let Some(tag) = &mut output {
					if !crate::remote::cache::token_valid(tag.tokens.local(), &self.server.clock) {
						tag.tokens.remove_local();
					}
					self.update_tokens_for_location(&mut tag.tokens, &location)?;
					tag.tokens.inherit(&tokens);
					tag.location = Some(location);
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
		let tag = tg::tag::Selector::Id(id.clone());
		let mut output = client
			.try_get_tag(&tag, arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote.name, "failed to get the tag"))?;
		let response =
			crate::remote::cache::Response::TagGet(crate::remote::cache::TagGetResponse {
				output: output.clone(),
			});
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await?;
		if let Some(tag) = &mut output {
			self.update_tokens_for_location(&mut tag.tokens, &location)?;
			tag.tokens.inherit(&tokens);
			tag.location = Some(location);
		}

		Ok(output)
	}

	pub(crate) async fn try_get_tag_request(
		&self,
		request: http::Request<BoxBody>,
		path: &[&str],
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
		let tag = path.join("/").replace(':', "/").parse()?;
		let Some(output) = self.try_get_tag(&tag, arg).await? else {
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
				let body = serde_json::to_vec(&output).unwrap();
				(Some(mime::APPLICATION_JSON), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
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
