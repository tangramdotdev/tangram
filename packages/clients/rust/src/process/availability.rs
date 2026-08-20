use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::is_false,
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	pub tokens: tg::authorization::Tokens,
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Availability {
	/// Whether this node's command's subtree is available.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_false")]
	pub node_command: bool,

	/// Whether this node's error's subtree is available.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 7, skip_serializing_if = "is_false")]
	pub node_error: bool,

	/// Whether this node's log's subtree is available.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_false")]
	pub node_log: bool,

	/// Whether this node's outputs' subtrees are available.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_false")]
	pub node_output: bool,

	/// Whether this node's subtree is available.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_false")]
	pub subtree: bool,

	/// Whether this node's subtree's commands' subtrees are available.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "is_false")]
	pub subtree_command: bool,

	/// Whether this node's subtree's errors' subtrees are available.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 8, skip_serializing_if = "is_false")]
	pub subtree_error: bool,

	/// Whether this node's subtree's logs' subtrees are available.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "is_false")]
	pub subtree_log: bool,

	/// Whether this node's subtree's outputs' subtrees are available.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "is_false")]
	pub subtree_output: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub location: Option<tg::location::Arg>,
}

impl Availability {
	pub fn merge(&mut self, other: &Self) {
		self.node_command = self.node_command || other.node_command;
		self.node_error = self.node_error || other.node_error;
		self.node_log = self.node_log || other.node_log;
		self.node_output = self.node_output || other.node_output;
		self.subtree = self.subtree || other.subtree;
		self.subtree_command = self.subtree_command || other.subtree_command;
		self.subtree_error = self.subtree_error || other.subtree_error;
		self.subtree_log = self.subtree_log || other.subtree_log;
		self.subtree_output = self.subtree_output || other.subtree_output;
	}
}

impl<O> tg::Process<O> {
	pub async fn availability(
		&self,
		options: tg::process::availability::Options,
	) -> tg::Result<tg::process::Availability> {
		let handle = tg::handle()?;
		self.availability_with_handle(handle, options).await
	}

	pub async fn availability_with_handle<H>(
		&self,
		handle: &H,
		options: tg::process::availability::Options,
	) -> tg::Result<tg::process::Availability>
	where
		H: tg::Handle,
	{
		self.try_get_availability_with_handle(handle, options)
			.await?
			.ok_or_else(|| tg::error!("failed to get the process availability"))
	}

	pub async fn try_get_availability(
		&self,
		options: tg::process::availability::Options,
	) -> tg::Result<Option<tg::process::Availability>> {
		let handle = tg::handle()?;
		self.try_get_availability_with_handle(handle, options).await
	}

	pub async fn try_get_availability_with_handle<H>(
		&self,
		handle: &H,
		options: tg::process::availability::Options,
	) -> tg::Result<Option<tg::process::Availability>>
	where
		H: tg::Handle,
	{
		let Some(id) = self.id().right() else {
			return Err(tg::error!(
				"getting the process availability is not supported for unsandboxed processes"
			));
		};
		let arg = tg::process::availability::Arg {
			location: options.location.or_else(|| self.location()),
			tokens: self.tokens(),
		};
		handle.try_get_process_availability(id, arg).await
	}
}

impl tg::Session {
	pub async fn try_get_process_availability(
		&self,
		id: &tg::process::Id,
		arg: tg::process::availability::Arg,
	) -> tg::Result<Option<tg::process::Availability>> {
		let method = http::Method::GET;
		let path = format!("/processes/{id}/availability");
		let uri = Uri::builder()
			.path(&path)
			.query_params_strict(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let availability = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;

		Ok(Some(availability))
	}
}
