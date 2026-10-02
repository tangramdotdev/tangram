use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _, future},
	serde_with::{DisplayFromStr, serde_as},
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
	pub force: bool,

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
	pub process_command_objects: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub process_error_objects: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub process_log_objects: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub process_output_objects: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub sandbox_processes: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	/// The source location. An absent source selects the default remote.
	pub source: Option<tg::Location>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub tag_targets: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub user_children: bool,
}

pub type Header = tg::push::Header;

pub type Output = tg::push::Output;

impl Default for Arg {
	fn default() -> Self {
		Self {
			ancestors: tg::node::AncestorsPull::default(),
			destination: Some(tg::Location::Local(tg::location::Local::default())),
			eager: true,
			force: false,
			group_children: false,
			nodes: Vec::new(),
			metadata: false,
			organization_children: false,
			process_children: false,
			process_command_objects: false,
			process_error_objects: true,
			process_log_objects: false,
			process_output_objects: true,
			sandbox_processes: false,
			source: None,
			tag_targets: true,
			user_children: false,
		}
	}
}

impl From<tg::pull::Arg> for tg::push::Arg {
	fn from(value: tg::pull::Arg) -> Self {
		Self {
			ancestors: value.ancestors,
			destination: value.destination,
			eager: value.eager,
			force: value.force,
			group_children: value.group_children,
			nodes: value.nodes,
			metadata: value.metadata,
			organization_children: value.organization_children,
			process_children: value.process_children,
			process_command_objects: value.process_command_objects,
			process_error_objects: value.process_error_objects,
			process_log_objects: value.process_log_objects,
			process_output_objects: value.process_output_objects,
			sandbox_processes: value.sandbox_processes,
			source: value.source,
			tag_targets: value.tag_targets,
			user_children: value.user_children,
		}
	}
}

impl tg::Session {
	pub async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<(
		tg::pull::Header,
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static + use<>,
	)> {
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
		// Read the header before consuming the progress stream.
		let mut reader = response.reader();
		let header =
			tangram_http::body::header::get(&mut reader, tangram_http::body::header::MAX_LENGTH)
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the header"))?;

		let stream = tangram_http::sse::decode(reader)
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
		Ok((header, stream))
	}
}
