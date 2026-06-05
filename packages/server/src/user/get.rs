use {
	crate::{Session, node::Node},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_get_user(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> tg::Result<Option<tg::User>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.try_get_user_local(user).await,
			tg::Location::Remote(remote) => {
				let client = self.get_remote_session(&remote.name).await?;
				let arg = tg::user::get::Arg {
					location: Some(tg::Location::Local(tg::location::Local::default()).into()),
				};
				client.try_get_user(user, arg).await
			},
		}
	}

	async fn try_get_user_local(&self, user: &tg::user::Selector) -> tg::Result<Option<tg::User>> {
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
		if node.kind != tg::id::Kind::User
			|| !self
				.node_is_visible_with_transaction(&transaction, &node.id)
				.await?
		{
			return Ok(None);
		}
		Self::user_from_node_with_transaction(&transaction, node)
			.await
			.map(Some)
	}

	pub(crate) async fn user_from_node_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		node: Node,
	) -> tg::Result<tg::User> {
		#[derive(db::row::Deserialize)]
		struct Row {
			email: String,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select email
				from user_emails
				where "user" = {p}1
				order by email;
			"#
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![node.id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(tg::User {
			emails: rows.into_iter().map(|row| row.email).collect(),
			id: node.id.try_into()?,
			location: None,
			name: node.name,
			specifier: node.specifier,
		})
	}

	pub(crate) async fn try_get_user_request(
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
			.unwrap_or(tg::user::get::Arg { location: None });
		let user = user.replace(':', "/").parse()?;
		let Some(output) = self.try_get_user(&user, arg).await? else {
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
