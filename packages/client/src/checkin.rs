use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _, future},
	serde_with::{DisplayFromStr, PickFirst, serde_as},
	std::{path::PathBuf, pin::pin},
	tangram_futures::stream::TryExt as _,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{CommaSeparatedString, is_false, is_true, return_false, return_true},
};

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	Hash,
	PartialEq,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Lock {
	#[default]
	Auto,
	Attr,
	File,
}

#[serde_as]
#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(flatten)]
	pub options: Options,

	pub path: PathBuf,

	#[serde_as(as = "CommaSeparatedString")]
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub updates: Vec<tg::tag::Pattern>,
}

#[serde_as]
#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Options {
	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub cache_pointers: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub destructive: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub deterministic: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub ignore: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub local_dependencies: bool,

	#[serde(default = "default_lock", skip_serializing_if = "is_default_lock")]
	pub lock: Option<Lock>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub locked: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub solve: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default = "return_false", skip_serializing_if = "is_false")]
	pub unsolved_dependencies: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<u64>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub watch: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub artifact: tg::Referent<tg::artifact::Id>,
}

pub async fn checkin<H>(handle: &H, arg: tg::checkin::Arg) -> tg::Result<tg::Artifact>
where
	H: tg::Handle,
{
	let stream = handle.checkin(arg).await?;
	let output = pin!(stream)
		.try_last()
		.await?
		.and_then(|event| event.try_unwrap_output().ok())
		.ok_or_else(|| tg::error!("stream ended without output"))?;
	let artifact = tg::Artifact::with_id(output.artifact.item);
	Ok(artifact)
}

impl tg::Client {
	pub async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		let method = http::Method::POST;
		let uri = "/checkin";
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

impl Default for Options {
	fn default() -> Self {
		Self {
			cache_pointers: true,
			destructive: false,
			deterministic: false,
			ignore: true,
			local_dependencies: true,
			lock: Some(Lock::default()),
			locked: false,
			solve: true,
			unsolved_dependencies: false,
			ttl: None,
			watch: false,
		}
	}
}

impl std::fmt::Display for Lock {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Auto => write!(f, "auto"),
			Self::Attr => write!(f, "attr"),
			Self::File => write!(f, "file"),
		}
	}
}

impl std::str::FromStr for Lock {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"auto" => Ok(Self::Auto),
			"attr" => Ok(Self::Attr),
			"file" => Ok(Self::File),
			_ => Err(tg::error!(%s, "invalid lock")),
		}
	}
}

#[expect(clippy::unnecessary_wraps)]
pub(crate) fn default_lock() -> Option<Lock> {
	Some(Lock::default())
}

#[expect(clippy::trivially_copy_pass_by_ref, clippy::ref_option)]
pub(crate) fn is_default_lock(lock: &Option<Lock>) -> bool {
	*lock == Some(Lock::Auto)
}
