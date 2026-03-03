use {
	super::Client,
	tangram_client::prelude::*,
	tangram_futures::{BoxAsyncRead, read::Ext as _},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct StdinArg {
	pub id: tg::process::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct StdoutArg {
	pub id: tg::process::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct StderrArg {
	pub id: tg::process::Id,
}

impl Client {
	pub async fn stdin(&self, arg: StdinArg, stdin: BoxAsyncRead<'static>) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/stdin?id={}", arg.id);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_OCTET_STREAM.to_string(),
			)
			.reader(stdin)
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		Ok(())
	}

	pub async fn stdout(&self, arg: StdoutArg) -> tg::Result<BoxAsyncRead<'static>> {
		let method = http::Method::GET;
		let uri = format!("/stdout?id={}", arg.id);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		let stdout = response.reader().boxed();
		Ok(stdout)
	}

	pub async fn stderr(&self, arg: StderrArg) -> tg::Result<BoxAsyncRead<'static>> {
		let method = http::Method::GET;
		let uri = format!("/stderr?id={}", arg.id);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		let stderr = response.reader().boxed();
		Ok(stderr)
	}
}
