use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _, future},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::{is_default, is_false},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub cached: bool,

	#[serde(default, skip_serializing_if = "is_default")]
	pub checkin: tg::checkin::Options,

	#[serde(default, skip_serializing_if = "is_default")]
	#[serde(flatten)]
	pub options: tg::reference::Options,

	#[serde(default, skip_serializing_if = "is_default")]
	pub ttl: tg::remote::cache::Ttl,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub referent: tg::Referent<tg::resolve::Node>,
}

pub type Node = tg::get::Node;

impl tg::Session {
	pub async fn try_resolve(
		&self,
		reference: &tg::Reference,
		mut arg: tg::resolve::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::resolve::Output>>>>
		+ Send
		+ 'static
		+ use<>,
	> {
		let method = http::Method::GET;
		arg.options = reference.options().clone();
		let path = format!("/resolve/{}", reference.node());
		let uri = Uri::builder()
			.path_raw(&path)
			.query_params_strict(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.empty()
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
		let content_type = response
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		if !matches!(
			content_type
				.as_ref()
				.map(|content_type| (content_type.type_(), content_type.subtype())),
			Some((mime::TEXT, mime::EVENT_STREAM)),
		) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}
		let stream = response
			.sse()
			.map_err(|error| tg::error!(!error, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			});

		Ok(stream)
	}
}
