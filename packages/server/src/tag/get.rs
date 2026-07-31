use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_database::prelude::*,
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
		if matches!(self.context.principal, tg::Principal::Process(_)) {
			return Err(tg::error!("unauthorized"));
		}
		let resource = tag.clone().into();
		let Some(item) = self
			.try_get_specifier(&resource, arg.location.as_ref(), arg.cached, arg.ttl)
			.await?
		else {
			return Ok(None);
		};
		let crate::get::SpecifierOutput {
			id,
			location,
			token,
		} = item;
		let Ok(id) = tg::tag::Id::try_from(id) else {
			return Ok(None);
		};
		let location =
			location.unwrap_or_else(|| tg::Location::Local(tg::location::Local::default()));
		match location {
			tg::Location::Local(_) => {
				let tag = tg::tag::Selector::Id(id);
				self.try_get_tag_local(&tag, token).await
			},
			tg::Location::Remote(remote) => self.try_get_tag_remote(&id, arg, remote, token).await,
		}
	}

	async fn try_get_tag_local(
		&self,
		tag: &tg::tag::Selector,
		token: Option<tg::grant::Token>,
	) -> tg::Result<Option<tg::tag::get::Output>> {
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
		let Some(node) =
			Self::try_get_specifier_by_selector_with_transaction(&transaction, tag).await?
		else {
			return Ok(None);
		};
		if node.kind() != tg::id::Kind::Tag {
			return Ok(None);
		}
		let visible = self
			.server
			.index
			.visible(std::slice::from_ref(&node.id), &self.context.principal)
			.await?
			.pop()
			.unwrap() || self
			.authorize(
				tg::grant::Resource::Id(node.id.clone()),
				tg::grant::Permission::Tag(tg::grant::permission::tag::Permission::Read),
			)
			.await?
			.is_some_and(|permissions| {
				permissions.contains(tg::grant::Permission::Tag(
					tg::grant::permission::tag::Permission::Read,
				))
			});
		if !visible {
			return Ok(None);
		}
		let data = Self::get_tag_data_with_transaction(&transaction, &node).await?;
		Ok(Some(tg::tag::get::Output {
			data,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			token,
		}))
	}

	async fn try_get_tag_remote(
		&self,
		id: &tg::tag::Id,
		mut arg: tg::tag::get::Arg,
		remote: tg::location::Remote,
		token: Option<tg::grant::Token>,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		let cached = arg.cached;
		let ttl = arg.ttl;
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
		if let Some(crate::remote::cache::Response::TagGet(mut output)) = self
			.try_get_cached_remote_response(&remote.name, &request, ttl)
			.await?
		{
			if let Some(tag) = &mut output {
				tag.token = tag.token.take().or_else(|| token.clone());
			}
			let valid = output
				.as_ref()
				.is_none_or(|tag| crate::remote::cache::token_valid(tag.token.as_ref()));
			if valid || cached {
				if let Some(tag) = &mut output {
					if !crate::remote::cache::token_valid(tag.token.as_ref()) {
						tag.token = None;
					}
					tag.location = Some(tg::Location::Remote(remote));
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
		if let Some(tag) = &mut output {
			tag.token = tag.token.take().or(token);
		}
		let response = crate::remote::cache::Response::TagGet(output.clone());
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await?;
		if let Some(tag) = &mut output {
			tag.location = Some(tg::Location::Remote(remote));
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
