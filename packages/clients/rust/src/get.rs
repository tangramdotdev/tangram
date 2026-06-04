use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _, future},
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::is_default,
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_default")]
	pub checkin: tg::checkin::Options,

	#[serde(default, skip_serializing_if = "is_default")]
	#[serde(flatten)]
	pub options: tg::reference::Options,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub referent: tg::Referent<tg::get::Item>,
}

#[derive(
	Clone,
	Debug,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(untagged)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Item {
	Id(tg::Id),
	Pointer(tg::graph::data::Pointer),
}

impl tg::Session {
	pub async fn try_get(
		&self,
		reference: &tg::Reference,
		mut arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>>
		+ Send
		+ 'static
		+ use<>,
	> {
		let method = http::Method::GET;
		arg.options = reference.options().clone();
		let path = format!("/_/{}", reference.item());
		let uri = Uri::builder()
			.path_raw(&path)
			.query_params(&arg)
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
