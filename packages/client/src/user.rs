use crate as tg;
use crate::{empty, Client, Id};
use http_body_util::BodyExt;
use tangram_error::{error, Result};
use url::Url;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Login {
	pub id: Id,
	pub url: Url,
	pub token: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct User {
	pub id: Id,
	pub email: String,
	pub token: Option<Id>,
}

impl Client {
	pub async fn create_login(&self) -> Result<tg::Login> {
		let method = http::Method::POST;
		let uri = "/logins";
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let response = serde_json::from_slice(&bytes)
			.map_err(|source| error!(!source, "failed to deserialize the response body"))?;
		Ok(response)
	}

	pub async fn get_login(&self, id: &tg::Id) -> Result<Option<tg::Login>> {
		let method = http::Method::GET;
		let uri = format!("/logins/{id}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| error!(!source, "failed to create the request"))?;
		let response = self
			.send(request)
			.await
			.map_err(|source| error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let response = serde_json::from_slice(&bytes)
			.map_err(|source| error!(!source, "failed to deserialize the response body"))?;
		Ok(response)
	}

	pub async fn get_user_for_token(&self, token: &str) -> Result<Option<tg::User>> {
		let method = http::Method::GET;
		let uri = "/user";
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::AUTHORIZATION, format!("Bearer {token}"))
			.body(body)
			.map_err(|source| error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let response = serde_json::from_slice(&bytes)
			.map_err(|source| error!(!source, "failed to deserialize the response body"))?;
		Ok(response)
	}
}
