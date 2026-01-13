use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _, future},
	std::ops::AddAssign,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::is_false,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub commands: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub errors: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub eager: bool,

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
	pub remote: Option<String>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub skipped: Amounts,
	pub transferred: Amounts,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Amounts {
	pub processes: u64,
	pub objects: u64,
	pub bytes: u64,
}

impl tg::Client {
	pub async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
	> {
		let method = http::Method::POST;
		let uri = "/push";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
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
			remote: None,
		}
	}
}

impl AddAssign<&tg::sync::ProgressMessage> for Output {
	fn add_assign(&mut self, other: &tg::sync::ProgressMessage) {
		self.skipped += &other.skipped;
		self.transferred += &other.transferred;
	}
}

impl AddAssign<&tg::sync::ProgressMessageAmounts> for Amounts {
	fn add_assign(&mut self, other: &tg::sync::ProgressMessageAmounts) {
		self.processes += other.processes;
		self.objects += other.objects;
		self.bytes += other.bytes;
	}
}
