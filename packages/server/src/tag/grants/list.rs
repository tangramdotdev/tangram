use {
	crate::{Session, context::Authentication, user::parse_selector},
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn list_tag_grants(
		&self,
		tag: &tg::tag::Selector,
		arg: tg::tag::grants::list::Arg,
	) -> tg::Result<Option<tg::tag::grants::list::Output>> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.list_tag_grants_local(tag).await,
			tg::Location::Remote(remote) => self.list_tag_grants_remote(tag, arg, remote).await,
		}
	}

	async fn list_tag_grants_local(
		&self,
		tag: &tg::tag::Selector,
	) -> tg::Result<Option<tg::tag::grants::list::Output>> {
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
		let Some(node) = Self::try_get_node_by_selector_with_transaction(&transaction, tag).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::Tag
			|| !self
				.node_is_visible_with_transaction(&transaction, &node.id)
				.await?
		{
			return Ok(None);
		}
		let data = Self::list_direct_grants_with_transaction(&transaction, &node.id).await?;
		Ok(Some(tg::tag::grants::list::Output { data }))
	}

	async fn list_tag_grants_remote(
		&self,
		tag: &tg::tag::Selector,
		mut arg: tg::tag::grants::list::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<tg::tag::grants::list::Output>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client.list_tag_grants(tag, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to list the tag grants"),
		)
	}

	pub(crate) async fn list_tag_grants_request(
		&self,
		request: http::Request<BoxBody>,
		tag: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or(tg::tag::grants::list::Arg { location: None });
		let tag = parse_selector::<tg::tag::Id>(tag)?;
		let Some(output) = self.list_tag_grants(&tag, arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
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
		Ok(response.body(body).unwrap())
	}
}
