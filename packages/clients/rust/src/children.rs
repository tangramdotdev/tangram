use {
	crate::prelude::*,
	serde_with::{DisplayFromStr, PickFirst, serde_as},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cursor: Option<String>,

	#[serde_as(as = "Option<PickFirst<(_, DisplayFromStr)>>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub limit: Option<u64>,

	pub node: tg::Referent<tg::Id>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cursor: Option<String>,

	pub data: Vec<tg::Referent<tg::Id>>,
}

impl tg::Session {
	pub async fn children(&self, arg: tg::children::Arg) -> tg::Result<tg::children::Output> {
		let method = http::Method::GET;
		let path = format!("/children/{}", arg.node.node);
		let uri = Uri::builder().path(&path).build().unwrap();
		#[derive(serde::Serialize)]
		struct Arg<'a> {
			#[serde(skip_serializing_if = "Option::is_none")]
			cursor: &'a Option<String>,
			#[serde(skip_serializing_if = "Option::is_none")]
			limit: Option<u64>,
			#[serde(flatten)]
			options: &'a tg::referent::Options,
		}
		let arg = Arg {
			cursor: &arg.cursor,
			limit: arg.limit,
			options: &arg.node.options,
		};
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.arg(&arg, tangram_http::body::Empty::new())
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			return Err(tg::error!(!error, status = %status, "the request failed"));
		}
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;

		Ok(output)
	}
}
