use crate::{
	self as tg,
	util::serde::{is_false, is_true, return_true},
};
use std::{collections::BTreeMap, path::PathBuf};
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checksum: Option<tg::Checksum>,

	pub command: Option<tg::command::Id>,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub create: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cwd: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub env: Option<BTreeMap<String, String>>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub mounts: Vec<tg::process::data::Mount>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub network: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::process::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub retry: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stderr: Option<tg::process::Io>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdin: Option<tg::process::Io>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdout: Option<tg::process::Io>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub process: tg::process::Id,
	pub remote: Option<String>,
}

impl tg::Client {
	pub async fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		let method = http::Method::POST;
		let uri = "/processes/spawn";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}

impl Default for Arg {
	fn default() -> Self {
		Self {
			checksum: None,
			command: None,
			create: true,
			cwd: None,
			env: None,
			mounts: Vec::new(),
			network: false,
			parent: None,
			remote: None,
			retry: false,
			stderr: None,
			stdin: None,
			stdout: None,
		}
	}
}
