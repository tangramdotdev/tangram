use {
	crate::prelude::*,
	serde_with::{DisplayFromStr, PickFirst, serde_as},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::is_false,
};

pub type Item = tg::Either<tg::sandbox::get::Output, tg::Referent<tg::sandbox::Id>>;

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub creator: Option<tg::Principal>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cursor: Option<String>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub expand: bool,

	#[serde_as(as = "Option<PickFirst<(_, DisplayFromStr)>>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub limit: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub owner: Option<tg::Principal>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub status: Option<tg::sandbox::Status>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cursor: Option<String>,

	pub data: Vec<Item>,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub creator: Option<tg::Principal>,
	pub cursor: Option<String>,
	pub expand: bool,
	pub limit: Option<u64>,
	pub location: Option<tg::location::Arg>,
	pub owner: Option<tg::Principal>,
	pub status: Option<tg::sandbox::Status>,
}

pub async fn list(options: tg::sandbox::list::Options) -> tg::Result<tg::sandbox::list::Output> {
	let instance = tg::instance()?;
	list_with_instance(instance, options).await
}

pub async fn list_with_instance<I>(
	instance: &I,
	options: tg::sandbox::list::Options,
) -> tg::Result<tg::sandbox::list::Output>
where
	I: tg::Instance,
{
	let arg = tg::sandbox::list::Arg {
		creator: options.creator,
		cursor: options.cursor,
		expand: options.expand,
		limit: options.limit,
		location: options.location,
		owner: options.owner,
		status: options.status,
	};
	instance.list_sandboxes(arg).await
}

impl tg::Session {
	pub async fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		let method = http::Method::GET;
		let uri = Uri::builder().path("/sandboxes").build().unwrap();
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
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;
		Ok(output)
	}
}
