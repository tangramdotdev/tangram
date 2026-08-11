use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _, future},
	serde_with::{DisplayFromStr, serde_as},
	std::ops::AddAssign,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{is_default, is_false},
};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_default")]
	pub ancestors: tg::node::AncestorsPull,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub destination: Option<tg::Location>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub eager: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub group_children: bool,

	#[serde_as(as = "Vec<DisplayFromStr>")]
	pub nodes: Vec<tg::Referent<tg::Id>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub metadata: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub organization_children: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub process_children: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub process_commands: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub process_errors: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub process_logs: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub process_outputs: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub sandbox_processes: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub source: Option<tg::Location>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub tag_targets: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub user_children: bool,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde_as(as = "Vec<DisplayFromStr>")]
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub nodes: Vec<tg::Referent<tg::Id>>,

	pub skipped: Amounts,
	pub transferred: Amounts,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Amounts {
	pub bytes: u64,
	pub groups: u64,
	pub objects: u64,
	pub organizations: u64,
	pub processes: u64,
	pub sandboxes: u64,
	pub tags: u64,
	pub users: u64,
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
			ancestors: tg::node::AncestorsPull::default(),
			destination: Some(tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			})),
			eager: true,
			group_children: false,
			nodes: Vec::new(),
			metadata: false,
			organization_children: false,
			process_children: false,
			process_commands: false,
			process_errors: false,
			process_logs: false,
			process_outputs: true,
			sandbox_processes: false,
			source: Some(tg::Location::Local(tg::location::Local::default())),
			tag_targets: true,
			user_children: false,
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
		self.bytes += other.bytes;
		self.groups += other.groups;
		self.objects += other.objects;
		self.organizations += other.organizations;
		self.processes += other.processes;
		self.sandboxes += other.sandboxes;
		self.tags += other.tags;
		self.users += other.users;
	}
}
