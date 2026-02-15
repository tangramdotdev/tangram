use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _, future},
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{CommaSeparatedString, is_false},
};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cached: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checksum: Option<tg::Checksum>,

	pub command: tg::Referent<tg::command::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub mounts: Vec<tg::process::data::Mount>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub network: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::process::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub retry: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stderr: Option<tg::process::Stdio>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdin: Option<tg::process::Stdio>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdout: Option<tg::process::Stdio>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub process: tg::process::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub wait: Option<tg::process::wait::Output>,
}

impl tg::Client {
	pub async fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>
		+ Send
		+ 'static,
	> {
		let method = http::Method::POST;
		let uri = "/processes/spawn";
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

impl Arg {
	#[must_use]
	pub fn with_command(command: tg::Referent<tg::command::Id>) -> Self {
		Self {
			cached: None,
			checksum: None,
			command,
			local: None,
			mounts: Vec::new(),
			network: false,
			parent: None,
			remotes: None,
			retry: false,
			stderr: None,
			stdin: None,
			stdout: None,
		}
	}

	#[must_use]
	pub fn with_command_and_checksum(
		command: tg::Referent<tg::command::Id>,
		checksum: Option<tg::Checksum>,
	) -> Self {
		Self {
			cached: None,
			checksum,
			command,
			local: None,
			mounts: Vec::new(),
			network: false,
			parent: None,
			remotes: None,
			retry: false,
			stderr: None,
			stdin: None,
			stdout: None,
		}
	}
}
