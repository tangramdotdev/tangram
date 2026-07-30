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
	) -> tg::Result<Option<tg::Group>> {
		let resource = group.clone().into();
		let Some(entry) = self
			.try_get_named_entry(&resource, arg.location.as_ref(), arg.cached, arg.ttl)
			.await?
		else {
			return Ok(None);
		};
		let tg::list::Entry::Group { id, location, .. } = entry else {
			return Ok(None);
		};
		let group = tg::group::Selector::Id(id);
		let location =
			location.unwrap_or_else(|| tg::Location::Local(tg::location::Local::default()));
		match location {
			tg::Location::Local(_) => self.try_get_group_local(&group).await,
			tg::Location::Remote(remote) => self.try_get_group_remote(&group, arg, remote).await,
		}
	}

	async fn try_get_group_local(
		&self,
		group: &tg::group::Selector,
	) -> tg::Result<Option<tg::Group>> {
		let permission =
			tg::grant::Permission::Group(tg::grant::permission::group::Permission::Read);
		let authorized = self.authorize(group.clone(), permission).await?;
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
		let Some(node) =
			Self::try_get_specifier_by_selector_with_transaction(&transaction, group).await?
		else {
			return Ok(None);
		};
		if node.kind() != tg::id::Kind::Group {
			return Ok(None);
		}
		Ok(Some(tg::Group {
			id: node.id.try_into()?,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			name: node.name,
			parent: node.parent,
			specifier: node.specifier,
		}))
	}

	async fn try_get_group_remote(
		&self,
		group: &tg::group::Selector,
		mut arg: tg::group::get::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<tg::Group>> {
		let request = crate::remote::cache::request("group.get", &(group, &remote.region));
		if let Some(mut output) = self
			.try_get_cached_remote_response::<Option<tg::Group>>(&remote.name, &request, arg.ttl)
			.await?
		{
			if let Some(group) = &mut output {
				group.location = Some(tg::Location::Remote(remote));
			}

			return Ok(output);
		}
		if arg.cached {
			return Ok(None);
		}
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.cached = false;
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		arg.ttl = None;
		let mut output = client.try_get_group(group, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the group"),
		)?;
		self.put_cached_remote_response(&remote.name, &request, &output)
			.await?;
		if let Some(group) = &mut output {
			group.location = Some(tg::Location::Remote(remote));
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
