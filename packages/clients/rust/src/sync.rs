use {
	crate::prelude::*,
	bytes::Bytes,
	futures::{prelude::*, stream::BoxStream},
	serde_with::{DisplayFromStr, PickFirst, serde_as},
	tangram_futures::{read::Ext as _, stream::Ext as _, task::Task, write::Ext as _},
	tangram_http::body::BodyStream,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::{
		BytesBase64, CommaSeparatedString, is_default, is_false, is_true, return_true,
	},
	tokio::io::AsyncReadExt as _,
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::io::StreamReader,
};

#[cfg(test)]
mod tests;

pub use id::Id;

pub mod control;
pub mod id;

pub mod flow;

pub const CONTENT_TYPE: &str = "application/vnd.tangram.sync";

#[derive(
	Clone,
	Copy,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
	#[tangram_serialize(id = 0)]
	pub limits: tangram_http::flow::Limits,

	#[tangram_serialize(id = 1)]
	pub max_frame_size: u64,

	#[tangram_serialize(id = 2)]
	pub max_message_size: u64,

	#[tangram_serialize(id = 3)]
	pub max_object_size: u64,
}

#[serde_as]
#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Arg {
	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "is_default")]
	pub ancestors: tg::node::AncestorsPull,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_false")]
	pub eager: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "is_false")]
	pub force: bool,

	#[serde_as(as = "CommaSeparatedString")]
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "Vec::is_empty")]
	pub get: Vec<tg::Referent<tg::Selector<tg::Id>>>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 4, default, skip_serializing_if = "is_false")]
	pub group_children: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 5, default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 6, default, skip_serializing_if = "is_false")]
	pub metadata: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 7, default, skip_serializing_if = "is_false")]
	pub organization_children: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 8, default, skip_serializing_if = "is_false")]
	pub process_children: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 9, default, skip_serializing_if = "is_false")]
	pub process_command_objects: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 10, default, skip_serializing_if = "is_false")]
	pub process_error_objects: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 11, default, skip_serializing_if = "is_false")]
	pub process_log_objects: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 12, default, skip_serializing_if = "is_false")]
	pub process_output_objects: bool,

	#[serde_as(as = "CommaSeparatedString")]
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(id = 13, default, skip_serializing_if = "Vec::is_empty")]
	pub put: Vec<tg::Referent<tg::Id>>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 14, default, skip_serializing_if = "is_false")]
	pub sandbox_processes: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 15, default, skip_serializing_if = "Option::is_none")]
	pub sync: Option<tg::Referent<tg::sync::Id>>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 16, default, skip_serializing_if = "is_false")]
	pub tag_targets: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 17, default, skip_serializing_if = "is_false")]
	pub user_children: bool,
}

#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Header {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub sync: Option<tg::Referent<tg::sync::Id>>,
}

#[derive(
	Clone,
	Debug,
	derive_more::TryUnwrap,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Message {
	#[tangram_serialize(id = 3)]
	Config(Config),

	#[tangram_serialize(id = 4)]
	Consumption(tangram_http::flow::Consumption),

	#[tangram_serialize(id = 2)]
	End,

	#[tangram_serialize(id = 0)]
	Get(GetMessage),

	#[tangram_serialize(id = 1)]
	Put(PutMessage),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum GetMessage {
	#[tangram_serialize(id = 1)]
	Available(GetAvailableMessage),

	#[tangram_serialize(id = 3)]
	End,

	#[tangram_serialize(id = 0)]
	Node(GetNodeMessage),

	#[tangram_serialize(id = 4)]
	Output(GetOutputMessage),

	#[tangram_serialize(id = 2)]
	Progress(ProgressMessage),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct GetNodeMessage {
	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	#[tangram_serialize(default = "return_true", id = 3, skip_serializing_if = "is_true")]
	pub descendants: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_false")]
	pub eager: bool,

	#[tangram_serialize(id = 0)]
	pub selector: tg::Selector<tg::Id>,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	#[tangram_serialize(
		default,
		id = 2,
		skip_serializing_if = "tg::authorization::Tokens::is_empty"
	)]
	pub tokens: tg::authorization::Tokens,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum GetAvailableMessage {
	#[tangram_serialize(id = 0)]
	Object(GetAvailableObjectMessage),

	#[tangram_serialize(id = 1)]
	Process(GetAvailableProcessMessage),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct GetAvailableObjectMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::object::Id,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct GetAvailableProcessMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::process::Id,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_false")]
	pub node_command_available: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 8, skip_serializing_if = "is_false")]
	pub node_error_available: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_false")]
	pub node_log_available: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_false")]
	pub node_output_available: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 7, skip_serializing_if = "is_false")]
	pub subtree_available: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "is_false")]
	pub subtree_command_available: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 9, skip_serializing_if = "is_false")]
	pub subtree_error_available: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "is_false")]
	pub subtree_log_available: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "is_false")]
	pub subtree_output_available: bool,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct GetOutputMessage {
	#[tangram_serialize(id = 0)]
	pub nodes: Vec<tg::Referent<tg::Id>>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum PutMessage {
	#[tangram_serialize(id = 3)]
	End,

	#[tangram_serialize(id = 1)]
	Missing(PutMissingMessage),

	#[tangram_serialize(id = 0)]
	Node(PutNodeMessage),

	#[tangram_serialize(id = 4)]
	Pending(tg::Id),

	#[tangram_serialize(id = 2)]
	Progress(ProgressMessage),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum PutNodeMessage {
	#[tangram_serialize(id = 0)]
	Group(PutNodeGroupMessage),

	#[tangram_serialize(id = 1)]
	Object(PutNodeObjectMessage),

	#[tangram_serialize(id = 2)]
	Organization(PutNodeOrganizationMessage),

	#[tangram_serialize(id = 3)]
	Process(PutNodeProcessMessage),

	#[tangram_serialize(id = 4)]
	Sandbox(PutNodeSandboxMessage),

	#[tangram_serialize(id = 5)]
	Tag(PutNodeTagMessage),

	#[tangram_serialize(id = 6)]
	User(PutNodeUserMessage),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PutNodeGroupMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::group::Id,

	#[tangram_serialize(id = 1)]
	pub name: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::Id>,

	#[tangram_serialize(id = 3)]
	pub specifier: tg::Specifier,
}

#[serde_as]
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PutNodeObjectMessage {
	#[tangram_serialize(id = 1)]
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,

	#[tangram_serialize(id = 0)]
	pub id: tg::object::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub metadata: Option<tg::object::Metadata>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PutNodeOrganizationMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::organization::Id,

	#[tangram_serialize(id = 1)]
	pub name: String,

	#[tangram_serialize(id = 2)]
	pub specifier: tg::Specifier,
}

#[serde_as]
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PutNodeProcessMessage {
	#[tangram_serialize(id = 1)]
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,

	#[tangram_serialize(id = 0)]
	pub id: tg::process::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub metadata: Option<tg::process::Metadata>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PutNodeSandboxMessage {
	#[tangram_serialize(id = 0)]
	pub created_at: i64,

	#[tangram_serialize(id = 1)]
	pub data: tg::sandbox::get::Output,

	#[tangram_serialize(id = 2)]
	pub id: tg::sandbox::Id,

	#[tangram_serialize(id = 3)]
	pub processes: Vec<tg::process::Id>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PutNodeTagMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::tag::Id,

	#[tangram_serialize(id = 2)]
	pub name: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::Id>,

	#[tangram_serialize(id = 4)]
	pub specifier: tg::Specifier,

	#[tangram_serialize(id = 1)]
	pub target: tg::Id,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Vec::is_empty")]
	pub tokens: Vec<tg::authorization::Token>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PutNodeUserMessage {
	#[tangram_serialize(id = 0)]
	pub emails: Vec<String>,

	#[tangram_serialize(id = 1)]
	pub id: tg::user::Id,

	#[tangram_serialize(id = 2)]
	pub name: String,

	#[tangram_serialize(id = 3)]
	pub specifier: tg::Specifier,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PutMissingMessage {
	#[tangram_serialize(id = 0)]
	pub selector: tg::Selector<tg::Id>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Vec::is_empty")]
	pub tokens: Vec<tg::authorization::Token>,
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
pub struct ProgressMessage {
	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_default")]
	pub skipped: ProgressMessageAmounts,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_default")]
	pub transferred: ProgressMessageAmounts,
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
pub struct ProgressMessageAmounts {
	#[serde(default, skip_serializing_if = "num::Zero::is_zero")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "num::Zero::is_zero")]
	pub bytes: u64,

	#[serde(default, skip_serializing_if = "num::Zero::is_zero")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "num::Zero::is_zero")]
	pub groups: u64,

	#[serde(default, skip_serializing_if = "num::Zero::is_zero")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "num::Zero::is_zero")]
	pub objects: u64,

	#[serde(default, skip_serializing_if = "num::Zero::is_zero")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "num::Zero::is_zero")]
	pub organizations: u64,

	#[serde(default, skip_serializing_if = "num::Zero::is_zero")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "num::Zero::is_zero")]
	pub processes: u64,

	#[serde(default, skip_serializing_if = "num::Zero::is_zero")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "num::Zero::is_zero")]
	pub sandboxes: u64,

	#[serde(default, skip_serializing_if = "num::Zero::is_zero")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "num::Zero::is_zero")]
	pub tags: u64,

	#[serde(default, skip_serializing_if = "num::Zero::is_zero")]
	#[tangram_serialize(default, id = 7, skip_serializing_if = "num::Zero::is_zero")]
	pub users: u64,
}

impl Config {
	pub fn validate(self) -> tg::Result<()> {
		self.limits
			.validate()
			.map_err(|error| tg::error!(source = error, "invalid sync limits"))?;
		if self.max_object_size == 0
			|| self.max_message_size < self.max_object_size.saturating_add(256)
			|| self.max_message_size > self.limits.bytes / 2
			|| self.max_message_size > self.max_frame_size
		{
			return Err(tg::error!("invalid sync message limits"));
		}
		Ok(())
	}

	pub fn sync_message_size(self, sync_message: &Message) -> tg::Result<usize> {
		let object_size = match sync_message {
			Message::Put(PutMessage::Node(PutNodeMessage::Object(sync_message))) => {
				Some(sync_message.bytes.len())
			},
			Message::Put(PutMessage::Node(PutNodeMessage::Process(sync_message))) => {
				Some(sync_message.bytes.len())
			},
			_ => None,
		};
		if object_size.is_some_and(|size| size as u64 > self.max_object_size) {
			return Err(tg::error!("the sync object size limit was exceeded"));
		}
		let hint = sync_message.size_hint();
		if hint.is_none() {
			let bytes = tangram_serialize::to_vec(sync_message)
				.map_err(|source| tg::error!(!source, "failed to serialize the sync message"))?;
			if bytes.len() as u64 > self.max_message_size {
				return Err(tg::error!("the sync message size limit was exceeded"));
			}
		}
		let size = hint.unwrap_or(self.max_message_size);
		if size > self.max_message_size {
			return Err(tg::error!("the sync message size limit was exceeded"));
		}
		let size = usize::try_from(size)
			.map_err(|error| tg::error!(!error, "invalid sync message size"))?;
		Ok(size)
	}
}

impl Message {
	#[must_use]
	pub fn size_hint(&self) -> Option<u64> {
		match self {
			Self::End | Self::Get(GetMessage::End) | Self::Put(PutMessage::End) => Some(64),
			Self::Get(
				GetMessage::Available(GetAvailableMessage::Process(_)) | GetMessage::Progress(_),
			)
			| Self::Put(PutMessage::Progress(_)) => Some(256),
			Self::Get(GetMessage::Available(GetAvailableMessage::Object(_)))
			| Self::Put(PutMessage::Pending(_)) => Some(128),
			Self::Get(GetMessage::Node(sync_message)) => {
				let selector_size =
					(sync_message.selector.to_string().len() as u64).checked_mul(8)?;
				256_u64
					.checked_add(selector_size)?
					.checked_add(tokens_size_hint(&sync_message.tokens)?)
			},
			Self::Put(PutMessage::Node(PutNodeMessage::Object(sync_message))) => {
				let metadata_size = if sync_message.metadata.is_some() {
					256
				} else {
					0
				};
				(sync_message.bytes.len() as u64).checked_add(256 + metadata_size)
			},
			Self::Put(PutMessage::Node(PutNodeMessage::Process(sync_message))) => {
				let metadata_size = if sync_message.metadata.is_some() {
					4096
				} else {
					0
				};
				(sync_message.bytes.len() as u64).checked_add(256 + metadata_size)
			},
			_ => None,
		}
	}
}

impl tg::Session {
	pub async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<(
		tg::sync::Header,
		impl Stream<Item = tg::Result<tg::sync::Message>> + Send + use<>,
	)> {
		let config = self.client().sync;
		let (mut connection, input) = flow::Connection::new(config)?;
		let stream = connection.send_sync_messages(stream);
		let max_frame_size = config.max_frame_size;
		let method = http::Method::POST;
		let uri = Uri::builder().path("/sync").build().unwrap();

		// Create the body.
		let stream = stream.then(move |result| async move {
			let frame = match result {
				Ok(message) => {
					let message = tangram_serialize::to_vec(&message).unwrap();
					let message_len = message.len();
					let len = u64::try_from(message_len).map_err(
						|error| tg::error!(!error, len = %message_len, "sync frame length out of range"),
					)?;
					if len > max_frame_size {
						return Err(tg::error!(
							len = %len,
							max = %max_frame_size,
							"sync frame too large"
						));
					}
					let mut bytes = Vec::with_capacity(9 + message.len());
					bytes.write_uvarint(len).await.unwrap();
					bytes.write_all(&message).await.unwrap();
					hyper::body::Frame::data(bytes.into())
				},
				Err(error) => {
					let mut trailers = http::HeaderMap::new();
					trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
					let json = error.state().object().map_or_else(
						|| serde_json::to_string(&error.id()).unwrap(),
						|object| serde_json::to_string(&object.to_data()).unwrap(),
					);
					trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
					hyper::body::Frame::trailers(trailers)
				},
			};
			Ok::<_, tg::Error>(frame)
		});
		let body = tangram_http::body::Boxed::with_stream(stream);

		// Send the request.
		let mut request = http::request::Builder::default();
		request = request
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, tg::sync::CONTENT_TYPE.to_string())
			.header(
				http::header::CONTENT_TYPE,
				tg::sync::CONTENT_TYPE.to_string(),
			);
		let request = request
			.arg_with_tangram(&arg, body)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send(request)
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

		// Validate the response content type.
		let content_type = response
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		if content_type.as_ref().is_some_and(|value| {
			value.type_() == mime::TEXT && value.subtype() == mime::EVENT_STREAM
		}) {
			let mut reader = response.reader();
			let header = tangram_http::body::header::get(
				&mut reader,
				tangram_http::body::header::MAX_LENGTH,
				tangram_http::body::encoding::Encoding::Json,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the header"))?;
			let stream = tangram_http::sse::decode(tokio::io::BufReader::new(reader))
				.map_err(|error| tg::error!(!error, "failed to read the sync message"))
				.and_then(|event| futures::future::ready(event.try_into()))
				.boxed();
			let task = connection.receive_sync_messages(stream);
			return Ok((header, input.attach(task).boxed()));
		}
		if content_type != Some(tg::sync::CONTENT_TYPE.parse().unwrap()) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}

		let mut stream = BodyStream::new(response.into_body());
		let (data_sender, data_receiver) = tokio::sync::mpsc::channel(1);
		let (trailer_sender, trailer_receiver) = tokio::sync::mpsc::channel(1);
		let task = Task::spawn(|_| async move {
			while let Some(result) = stream.next().await {
				match result {
					Ok(frame) => {
						if frame.is_data() {
							let data = frame.into_data().unwrap();
							data_sender.send(Ok(data)).await.ok();
						} else if frame.is_trailers() {
							let trailers = frame.into_trailers().unwrap();
							trailer_sender.send(trailers).await.ok();
						} else {
							unreachable!()
						}
					},
					Err(error) => {
						data_sender.send(Err(error)).await.ok();
					},
				}
			}
		});

		let mut reader =
			StreamReader::new(ReceiverStream::new(data_receiver).map_err(std::io::Error::other));
		let header = tangram_http::body::header::get(
			&mut reader,
			tangram_http::body::header::MAX_LENGTH,
			tangram_http::body::encoding::Encoding::Tangram,
		)
		.await
		.map_err(|error| tg::error!(!error, "failed to deserialize the header"))?;
		let data_messages = stream::try_unfold(reader, move |mut reader| async move {
			let Some(len) = reader
				.try_read_uvarint()
				.await
				.map_err(|error| tg::error!(!error, "failed to read the length"))?
			else {
				return Ok(None);
			};
			if len > max_frame_size {
				return Err(tg::error!(
					len = %len,
					max = %max_frame_size,
					"sync frame too large"
				));
			}
			let len = usize::try_from(len).map_err(
				|error| tg::error!(!error, len = %len, "sync frame length out of range"),
			)?;
			let mut bytes = vec![0; len];
			reader
				.read_exact(&mut bytes)
				.await
				.map_err(|error| tg::error!(!error, "failed to read the message"))?;
			let message = tangram_serialize::from_slice(&bytes)
				.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
			Ok(Some((message, reader)))
		});

		let trailers = ReceiverStream::new(trailer_receiver);
		let trailer_messages = trailers.then(|trailers| async move {
			let event = trailers
				.get("x-tg-event")
				.ok_or_else(|| tg::error!("missing event"))?
				.to_str()
				.map_err(|error| tg::error!(!error, "invalid event"))?;
			if let "error" = event {
				let data = trailers
					.get("x-tg-data")
					.ok_or_else(|| tg::error!("missing data"))?
					.to_str()
					.map_err(|error| tg::error!(!error, "invalid data"))?;
				let error = serde_json::from_str(data).map_err(|error| {
					tg::error!(!error, "failed to deserialize the header value")
				})?;
				Err(error)
			} else {
				Err(tg::error!("invalid event"))
			}
		});

		let stream = stream::select(data_messages, trailer_messages)
			.attach(task)
			.boxed();

		let task = connection.receive_sync_messages(stream);
		Ok((header, input.attach(task).boxed()))
	}
}

impl TryFrom<Message> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(message: Message) -> tg::Result<Self> {
		let (event, data) = match message {
			Message::Config(message) => ("config", serde_json::to_string(&message)),
			Message::Consumption(message) => ("consumption", serde_json::to_string(&message)),
			Message::End => ("end", serde_json::to_string(&())),
			Message::Get(message) => ("get", serde_json::to_string(&message)),
			Message::Put(message) => ("put", serde_json::to_string(&message)),
		};
		let data =
			data.map_err(|error| tg::error!(!error, "failed to serialize the sync message"))?;
		let event = Some(event.to_owned());
		let event = Self {
			data,
			event,
			..Default::default()
		};

		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for Message {
	type Error = tg::Error;

	fn try_from(event: tangram_http::sse::Event) -> tg::Result<Self> {
		let message = match event.event.as_deref() {
			Some("config") => serde_json::from_str(&event.data).map(Self::Config),
			Some("consumption") => serde_json::from_str(&event.data).map(Self::Consumption),
			Some("end") => serde_json::from_str::<()>(&event.data).map(|()| Self::End),
			Some("error") => {
				let error: tg::Either<tg::error::Data, tg::error::Id> =
					serde_json::from_str(&event.data).map_err(|error| {
						tg::error!(!error, "failed to deserialize the sync error")
					})?;
				return Err(error.try_into()?);
			},
			Some("get") => serde_json::from_str(&event.data).map(Self::Get),
			Some("put") => serde_json::from_str(&event.data).map(Self::Put),
			_ => return Err(tg::error!("invalid sync message")),
		}
		.map_err(|error| tg::error!(!error, "failed to deserialize the sync message"))?;

		Ok(message)
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			limits: tangram_http::flow::Limits {
				bytes: 2 * 1024 * 1024,
				messages: 1024,
			},
			max_frame_size: 64 * 1024 * 1024,
			max_message_size: 512 * 1024,
			max_object_size: 256 * 1024,
		}
	}
}

fn tokens_size_hint(tokens: &tg::authorization::Tokens) -> Option<u64> {
	tokens.iter().try_fold(0_u64, |size, (location, entry)| {
		let location_size = (location.to_string().len() as u64).checked_mul(8)?;
		let size = size.checked_add(256)?.checked_add(location_size)?;
		entry.authorization.iter().try_fold(size, |size, token| {
			let key_size = (token.metadata.key.len() as u64).checked_mul(8)?;
			let permission_size = (token.body.permissions.len() as u64).checked_mul(128)?;
			let signature_size = (token.signature.len() as u64).checked_mul(2)?;
			size.checked_add(1024)?
				.checked_add(key_size)?
				.checked_add(permission_size)?
				.checked_add(signature_size)
		})
	})
}
