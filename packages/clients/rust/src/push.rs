use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _, future},
	serde_with::{DisplayFromStr, serde_as},
	std::ops::AddAssign,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::is_false,
};

#[serde_as]
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

	#[serde_as(as = "Vec<DisplayFromStr>")]
	pub items: Vec<tg::Referent<tg::Either<tg::object::Id, tg::process::Id>>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub logs: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub metadata: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub outputs: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub recursive: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub destination: Option<tg::Location>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub source: Option<tg::Location>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde_as(as = "Vec<DisplayFromStr>")]
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub items: Vec<tg::Referent<tg::Either<tg::object::Id, tg::process::Id>>>,

	pub skipped: Amounts,
	pub transferred: Amounts,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Amounts {
	pub processes: u64,
	pub objects: u64,
	pub bytes: u64,
}

impl tg::Session {
	pub async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static + use<>,
	> {
		let method = http::Method::POST;
		let uri = "/push";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
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
			destination: Some(tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			})),
			source: Some(tg::Location::Local(tg::location::Local::default())),
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
