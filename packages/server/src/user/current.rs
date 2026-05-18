use {
	crate::{Session, context::Authentication},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> tg::Result<Option<tg::user::User>> {
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
			.identity_location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		let output = match location {
			tg::Location::Local(_) => match &self.context.authentication {
				Some(Authentication::User(user)) => Some(user.clone()),
				_ => None,
			},
			tg::Location::Remote(remote) => self.get_current_user_remote(remote).await?,
		};
		let Some(output) = output else {
			return Ok(None);
		};

		Ok(Some(output))
	}

	async fn get_current_user_remote(
		&self,
		remote: tg::location::Remote,
	) -> tg::Result<Option<tg::user::User>> {
		let client = self
			.get_remote_session(remote.name.clone())
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					remote = %remote.name,
					"failed to get the remote client"
				)
			})?;
		let arg = tg::user::current::Arg {
			location: Some(tg::Location::Local(tg::location::Local::default()).into()),
		};
		let mut user = client.get_current_user(arg).await.map_err(|error| {
			tg::error!(
				!error,
				remote = %remote.name,
				"failed to get the current user"
			)
		})?;
		if let Some(user) = &mut user {
			user.location = Some(tg::Location::Remote(tg::location::Remote {
				name: remote.name,
				region: None,
			}));
		}
		Ok(user)
	}

	pub(crate) async fn get_current_user_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the current user.
		let Some(output) = self.get_current_user(arg).await? else {
			let response = http::Response::builder()
				.status(http::StatusCode::UNAUTHORIZED)
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};

		// Create the response.
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
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
