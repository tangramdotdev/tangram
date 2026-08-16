use {
	crate::prelude::*,
	futures::{Stream, StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	serde_with::{DisplayFromStr, serde_as},
	std::path::PathBuf,
	tangram_futures::stream::TryExt as _,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{is_false, is_true, return_true},
};

pub use crate::checkin::Lock;

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub dependencies: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub extension: Option<String>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub force: bool,

	#[serde(
		default = "tg::checkin::default_lock",
		skip_serializing_if = "tg::checkin::is_default_lock"
	)]
	pub lock: Option<Lock>,

	#[serde_as(as = "Vec<DisplayFromStr>")]
	pub nodes: Vec<tg::Referent<tg::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub paths: Vec<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct Options {
	pub dependencies: bool,
	pub extension: Option<String>,
	pub force: bool,
	pub lock: Option<Lock>,
	pub path: Option<PathBuf>,
}

pub async fn checkout(arg: Arg) -> tg::Result<Vec<PathBuf>> {
	let handle = tg::handle()?;
	checkout_with_handle(handle, arg).await
}

pub async fn checkout_with_handle<H>(handle: &H, arg: Arg) -> tg::Result<Vec<PathBuf>>
where
	H: tg::Handle,
{
	let stream = handle.checkout(arg).await?.boxed();
	let output = stream
		.try_last()
		.await?
		.and_then(|event| event.try_unwrap_output().ok())
		.ok_or_else(|| tg::error!("stream ended without output"))?;
	Ok(output.paths)
}

pub async fn checkout_one_with_handle<H>(handle: &H, arg: Arg) -> tg::Result<PathBuf>
where
	H: tg::Handle,
{
	let mut paths = checkout_with_handle(handle, arg).await?;
	if paths.len() != 1 {
		return Err(tg::error!("expected exactly one checkout path"));
	}
	Ok(paths.pop().unwrap())
}

impl tg::Artifact {
	pub async fn checkout(
		&self,
		options: tg::checkout::Options,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkout::Output>>>> {
		let handle = tg::handle()?;
		self.checkout_with_handle(handle, options).await
	}

	pub async fn checkout_with_handle<H>(
		&self,
		handle: &H,
		options: tg::checkout::Options,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkout::Output>>>>
	where
		H: tg::Handle,
	{
		let arg = tg::checkout::Arg {
			dependencies: options.dependencies,
			extension: options.extension,
			force: options.force,
			lock: options.lock,
			nodes: vec![self.to_referent().map(Into::into)],
			path: options.path,
		};
		let stream = handle.checkout(arg).await?.boxed();

		Ok(stream)
	}
}

impl tg::Session {
	pub async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>>
		+ Send
		+ 'static
		+ use<>,
	> {
		let method = http::Method::POST;
		let uri = "/checkout";
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

impl Default for Options {
	fn default() -> Self {
		Self {
			dependencies: true,
			extension: None,
			force: false,
			lock: Some(Lock::default()),
			path: None,
		}
	}
}
