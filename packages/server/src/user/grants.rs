use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_get_user_grants(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::grants::Arg,
	) -> tg::Result<Option<tg::user::grants::Output>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.try_get_user_grants_local(user).await,
			tg::Location::Remote(remote) => {
				let client = self.get_remote_session(&remote.name).await?;
				let arg = tg::user::grants::Arg {
					location: Some(tg::Location::Local(tg::location::Local::default()).into()),
				};
				client.try_get_user_grants(user, arg).await
			},
		}
	}

	async fn try_get_user_grants_local(
		&self,
		user: &tg::user::Selector,
	) -> tg::Result<Option<tg::user::grants::Output>> {
		let authorized = self
			.authorize(user.clone().into(), tg::grant::Permission::Admin)
			.await?;
		if authorized != Some(true) {
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
			Self::try_get_node_by_selector_with_transaction(&transaction, user).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::User {
			return Ok(None);
		}
		let data = Self::list_direct_grants_with_transaction(&transaction, &node.id).await?;
		Ok(Some(tg::user::grants::Output { data }))
	}

	pub(crate) async fn try_get_user_grants_request(
		&self,
		request: http::Request<BoxBody>,
		user: &str,
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
		let user = user.replace(':', "/").parse()?;
		let Some(output) = self.try_get_user_grants(&user, arg).await? else {
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
