use std::path::PathBuf;

use crate::{
	self as tg,
	handle::Ext as _,
	util::serde::{is_false, is_true, return_true},
};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub create: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::process::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub retry: bool,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub volumes: Vec<PathBuf>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub ports: Vec<(u16, u16)>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub process: tg::process::Id,
	pub remote: Option<String>,
	pub token: Option<String>,
}

impl tg::Command {
	pub async fn spawn<H>(
		&self,
		handle: &H,
		arg: tg::command::spawn::Arg,
	) -> tg::Result<tg::Process>
	where
		H: tg::Handle,
	{
		let id = self.id(handle).await?;
		let output = handle.spawn_command(&id, arg).await?;
		let process = tg::Process::with_id(output.process);
		Ok(process)
	}

	pub async fn output<H>(&self, handle: &H, arg: tg::command::spawn::Arg) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let process = self.spawn(handle, arg).await?;
		let output = process.output(handle).await?;
		Ok(output)
	}
}

impl tg::Client {
	pub async fn try_spawn_command(
		&self,
		id: &tg::command::Id,
		arg: tg::command::spawn::Arg,
	) -> tg::Result<Option<tg::command::spawn::Output>> {
		let method = http::Method::POST;
		let uri = format!("/commands/{id}/spawn");
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
			create: true,
			parent: None,
			remote: None,
			retry: false,
			volumes: Vec::new(),
			ports: Vec::new(),
		}
	}
}
