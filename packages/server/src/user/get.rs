use {
	crate::{Session, context::Authentication},
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
		arg: tg::user::get::Arg,
	) -> tg::Result<Option<tg::User>> {
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
			tg::Location::Local(_) => self.try_get_user_local(&arg.namespace).await,
			tg::Location::Remote(remote) => self.try_get_user_remote(arg, remote).await,
		}
	}

	async fn try_get_user_local(&self, user: &str) -> tg::Result<Option<tg::User>> {
		if self
			.context
			.authentication
			.as_ref()
			.is_none_or(|authentication| authentication.is_runner() || authentication.is_sandbox())
		{
			return Err(tg::error!("unauthorized"));
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
		let output = Self::try_get_user_with_transaction(&transaction, user).await?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

		Ok(output)
	}

	async fn try_get_user_remote(
		&self,
		mut arg: tg::user::get::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<tg::User>> {
		let client = self
			.get_remote_session(&remote.name)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					remote = %remote.name,
					"failed to get the remote client"
				)
			})?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let mut output = client.try_get_user(arg).await.map_err(|error| {
			tg::error!(
				!error,
				remote = %remote.name,
				"failed to get the user"
			)
		})?;
		if let Some(output) = &mut output {
			output.location = Some(tg::Location::Remote(tg::location::Remote {
				name: remote.name,
				region: None,
			}));
		}
		Ok(output)
	}

	pub(crate) async fn try_get_user_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		user: &str,
	) -> tg::Result<Option<tg::User>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			email: Option<String>,
			namespace: Option<String>,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::user::Id,
		}

		let p = transaction.p();
		let (where_, param) = if let Ok(id) = user.parse::<tg::user::Id>() {
			("users.id", id.to_string())
		} else {
			("users.namespace", user.to_owned())
		};
		let statement = formatdoc!(
			r#"
				select users.id, users.namespace, user_emails.email
				from users
				left join user_emails on user_emails."user" = users.id
				where {where_} = {p}1
				order by user_emails.email;
			"#
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![param])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(first) = rows.first() else {
			return Ok(None);
		};
		let id = first.id.clone();
		let namespace = first
			.namespace
			.clone()
			.map(|namespace| namespace.parse())
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the namespace"))?;
		let user = tg::User {
			id,
			emails: rows.into_iter().filter_map(|row| row.email).collect(),
			namespace,
			location: None,
		};
		Ok(Some(user))
	}

	pub(crate) async fn try_get_user_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params::<tg::user::get::Arg>()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("expected query params"))?;
		let namespace = arg.namespace.clone();
		let Some(output) = self.try_get_user(arg).await.map_err(
			|error| tg::error!(!error, namespace = %namespace, "failed to get the user"),
		)?
		else {
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
