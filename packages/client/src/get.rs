use {
	crate as tg,
	futures::{Stream, TryStreamExt as _, future},
	tangram_either::Either,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub referent: tg::Referent<Either<tg::process::Id, tg::object::Id>>,
}

impl tg::Client {
	pub async fn try_get(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + 'static,
	> {
		let method = http::Method::GET;
		let uri = reference.to_uri();
		let path = uri.path();
		let path = format!("/_/{path}");
		let uri = uri.to_builder().path(path).build().unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri.to_string())
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
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
			.map_err(|source| tg::error!(!source, "failed to read an event"))
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
