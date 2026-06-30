use {
	crate::prelude::*,
	std::time::Duration,
	tangram_http::{body::Bytes as BytesBody, response::Ext as _},
};

pub mod device_session {
	#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
	pub struct Arg {
		pub client_id: String,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub name: Option<crate::Specifier>,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub scope: Option<String>,
	}

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Output {
		pub device_code: String,
		pub user_code: String,
		pub verification_uri: String,
		pub verification_uri_complete: String,
		pub expires_in: u64,
		pub interval: u64,
	}
}

pub mod token {
	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Arg {
		pub client_id: String,
		pub device_code: String,
		pub grant_type: String,
	}

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	#[serde(untagged)]
	pub enum Output {
		Token(Token),
		Error(Error),
	}

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Token {
		pub access_token: String,
		pub token_type: String,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub expires_in: Option<u64>,
	}

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Error {
		pub error: String,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub error_description: Option<String>,
	}
}

impl tg::Client {
	pub async fn create_oauth_device_session(
		&self,
		arg: tg::oauth::device_session::Arg,
	) -> tg::Result<tg::oauth::device_session::Output> {
		let body = form_encode(&[
			("client_id", Some(arg.client_id.as_str())),
			(
				"name",
				arg.name.as_ref().map(ToString::to_string).as_deref(),
			),
			("scope", arg.scope.as_deref()),
		]);
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri("/oauth/device_authorization")
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.header(
				http::header::CONTENT_TYPE,
				"application/x-www-form-urlencoded",
			)
			.body(BytesBody::new(body))
			.unwrap();
		let response = self.send_oauth_request(request).await?;
		response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))
	}

	pub async fn get_oauth_device_access_token(
		&self,
		arg: tg::oauth::token::Arg,
	) -> tg::Result<tg::oauth::token::Output> {
		let body = form_encode(&[
			("grant_type", Some(arg.grant_type.as_str())),
			("device_code", Some(arg.device_code.as_str())),
			("client_id", Some(arg.client_id.as_str())),
		]);
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri("/oauth/token")
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.header(
				http::header::CONTENT_TYPE,
				"application/x-www-form-urlencoded",
			)
			.body(BytesBody::new(body))
			.unwrap();
		let response = self.send_oauth_request(request).await?;
		response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))
	}

	async fn send_oauth_request(
		&self,
		request: http::Request<BytesBody>,
	) -> tg::Result<http::Response<tangram_http::body::Boxed>> {
		let response = self
			.session(self.context())
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if !response.status().is_success()
			&& response.status() != http::StatusCode::BAD_REQUEST
			&& response.status() != http::StatusCode::FORBIDDEN
		{
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			return Err(tg::error!(!error, status = %status, "the request failed"));
		}
		Ok(response)
	}
}

pub async fn poll_device_access_token(
	client: &tg::Client,
	device_code: String,
	client_id: String,
	interval: Duration,
) -> tg::Result<tg::oauth::token::Token> {
	loop {
		tokio::time::sleep(interval).await;
		let output = client
			.get_oauth_device_access_token(tg::oauth::token::Arg {
				client_id: client_id.clone(),
				device_code: device_code.clone(),
				grant_type: "urn:ietf:params:oauth:grant-type:device_code".to_owned(),
			})
			.await?;
		match output {
			tg::oauth::token::Output::Token(token) => return Ok(token),
			tg::oauth::token::Output::Error(error) if error.error == "authorization_pending" => {},
			tg::oauth::token::Output::Error(error) => {
				return Err(tg::error!(
					error = %error.error,
					description = ?error.error_description,
					"failed to get the access token"
				));
			},
		}
	}
}

fn form_encode(params: &[(&str, Option<&str>)]) -> String {
	params
		.iter()
		.filter_map(|(key, value)| value.map(|value| (*key, value)))
		.map(|(key, value)| {
			let key = tangram_uri::encode_query_value(key);
			let value = tangram_uri::encode_query_value(value);
			format!("{key}={value}")
		})
		.collect::<Vec<_>>()
		.join("&")
}
