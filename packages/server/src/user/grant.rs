use {
	crate::Session,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Session {
	pub(crate) async fn grant_user_namespace_permission(
		&self,
		user: &str,
		arg: tg::user::grant::Arg,
	) -> tg::Result<tg::Grant> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		self.authorize()
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize"))?;
		let created_by = self.context.user.as_ref().map(|user| user.id.clone());

		let mut connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let user = Self::try_get_user_with_transaction(&transaction, user)
			.await?
			.ok_or_else(|| tg::error!("failed to find the user"))?;
		let namespace_id =
			Self::get_or_create_namespace_with_transaction(&transaction, &arg.namespace).await?;
		let grant = Self::grant_namespace_to_user_with_transaction(
			&transaction,
			&arg.namespace,
			namespace_id,
			&user.id,
			arg.permission,
			created_by.as_ref(),
		)
		.await?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(grant)
	}

	pub(crate) async fn try_get_user_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		user: &str,
	) -> tg::Result<Option<tg::User>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			email: Option<String>,
			handle: Option<String>,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::user::Id,
		}

		let p = transaction.p();
		let (where_, param) = if let Ok(id) = user.parse::<tg::user::Id>() {
			("users.id", id.to_string())
		} else {
			("users.handle", user.to_owned())
		};
		let statement = formatdoc!(
			"
				select users.id, users.handle, user_emails.email
				from users
				left join user_emails on user_emails.\"user\" = users.id
				where {where_} = {p}1
				order by user_emails.email;
			"
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![param])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(first) = rows.first() else {
			return Ok(None);
		};
		let id = first.id.clone();
		let handle = first.handle.clone();
		let user = tg::User {
			id,
			emails: rows.into_iter().filter_map(|row| row.email).collect(),
			handle,
			location: None,
		};
		Ok(Some(user))
	}

	pub(crate) async fn grant_user_namespace_permission_request(
		&self,
		request: http::Request<BoxBody>,
		user: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self
			.grant_user_namespace_permission(user, arg)
			.await
			.map_err(
				|error| tg::error!(!error, %user, "failed to grant the namespace permission"),
			)?;
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
