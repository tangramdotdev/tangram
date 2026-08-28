use {
	super::sign::Request,
	super::{Client, Session},
	aws_credential_types::Credentials,
	bytes::Bytes,
	tangram_client::prelude::*,
	time::{OffsetDateTime, format_description::well_known::Rfc3339},
};

const REFRESH_BUFFER: time::Duration = time::Duration::minutes(1);

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateSessionOutput {
	credentials: CreateSessionCredentials,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateSessionCredentials {
	access_key_id: String,
	expiration: String,
	secret_access_key: String,
	session_token: String,
}

impl Client {
	pub(super) async fn signing_credentials(&self) -> tg::Result<(Credentials, Option<String>)> {
		let Some(session) = &self.session else {
			return Ok((self.credentials.clone(), None));
		};
		let mut session = session.lock().await;
		let now = OffsetDateTime::now_utc();
		if let Some(session) = session
			.as_ref()
			.filter(|session| session.expiration > now + REFRESH_BUFFER)
		{
			return Ok((session.credentials.clone(), Some(session.token.clone())));
		}
		let next = self.create_session().await?;
		let output = (next.credentials.clone(), Some(next.token.clone()));
		*session = Some(next);

		Ok(output)
	}

	async fn create_session(&self) -> tg::Result<Session> {
		let arg = Request {
			body: Bytes::new(),
			credentials: &self.credentials,
			headers: http::HeaderMap::new(),
			method: http::Method::GET,
			path: "/",
			query: Some("session"),
			session_token: None,
		};
		let request = self.request(arg)?;
		let response = self.send_request(request).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to send the S3 Express CreateSession request"
			)
		})?;
		if !response.status.is_success() {
			return Err(tg::error!(
				status = %response.status,
				body = %String::from_utf8_lossy(&response.bytes),
				"failed to create an S3 Express session"
			));
		}
		parse_session(&response.bytes)
	}
}

fn parse_session(bytes: &[u8]) -> tg::Result<Session> {
	let output: CreateSessionOutput = quick_xml::de::from_reader(bytes)
		.map_err(|error| tg::error!(!error, "failed to parse the S3 Express session response"))?;
	let credentials = output.credentials;
	if credentials.access_key_id.is_empty()
		|| credentials.secret_access_key.is_empty()
		|| credentials.session_token.is_empty()
	{
		return Err(tg::error!(
			"the S3 Express session response contains empty credentials"
		));
	}
	let expiration = OffsetDateTime::parse(&credentials.expiration, &Rfc3339)
		.map_err(|error| tg::error!(!error, "failed to parse the S3 Express session expiration"))?;
	if expiration <= OffsetDateTime::now_utc() {
		return Err(tg::error!(
			"the S3 Express session response is already expired"
		));
	}
	let session = Session {
		credentials: Credentials::new(
			credentials.access_key_id,
			credentials.secret_access_key,
			None,
			None,
			"tangram S3 Express session",
		),
		expiration,
		token: credentials.session_token,
	};

	Ok(session)
}

#[cfg(test)]
mod tests {
	#[test]
	fn parses_a_session() {
		let expiration = (time::OffsetDateTime::now_utc() + time::Duration::minutes(5))
			.format(&time::format_description::well_known::Rfc3339)
			.unwrap();
		let response = format!(
			"<CreateSessionResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Credentials><AccessKeyId>access</AccessKeyId><SecretAccessKey>secret</SecretAccessKey><SessionToken>token</SessionToken><Expiration>{expiration}</Expiration></Credentials></CreateSessionResult>"
		);
		let session = super::parse_session(response.as_bytes()).unwrap();
		assert_eq!(session.credentials.access_key_id(), "access");
		assert_eq!(session.credentials.secret_access_key(), "secret");
		assert_eq!(session.token, "token");
	}
}
