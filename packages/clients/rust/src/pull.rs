use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _, future},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::is_false,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub commands: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub eager: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub errors: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub force: bool,

	pub items: Vec<tg::Either<tg::object::Id, tg::process::Id>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub logs: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub metadata: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub outputs: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub recursive: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub destination: Option<tg::location::Location>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub source: Option<tg::location::Location>,
}

pub type Output = tg::push::Output;

impl Default for Arg {
	fn default() -> Self {
		Self {
			commands: false,
			eager: true,
			errors: false,
			force: false,
			items: Vec::new(),
			logs: false,
			metadata: false,
			outputs: true,
			recursive: false,
			destination: Some(tg::location::Location::Local(tg::location::Local::default())),
			source: Some(tg::location::Location::Remote(tg::location::Remote {
				remote: "default".to_owned(),
				regions: None,
			})),
		}
	}
}

impl From<tg::pull::Arg> for tg::push::Arg {
	fn from(value: tg::pull::Arg) -> Self {
		Self {
			commands: value.commands,
			eager: value.eager,
			errors: value.errors,
			force: value.force,
			items: value.items,
			logs: value.logs,
			metadata: value.metadata,
			outputs: value.outputs,
			recursive: value.recursive,
			destination: value.destination,
			source: value.source,
		}
	}
}

impl tg::Client {
	pub async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
	> {
		let method = http::Method::POST;
		let uri = "/pull";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
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
