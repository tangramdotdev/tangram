use {
	crate::prelude::*,
	bytes::Bytes,
	futures::{prelude::*, stream::BoxStream},
	serde_with::{DisplayFromStr, PickFirst, serde_as},
	tangram_futures::{read::Ext as _, stream::Ext as _, task::Task, write::Ext as _},
	tangram_http::body::BodyStream,
	tangram_http::response::Ext as _,
	tangram_uri::{Uri, builder::QueryParamsError},
	tangram_util::serde::{CommaSeparatedString, is_default, is_false, is_true, return_true},
	tokio::io::AsyncReadExt as _,
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::io::StreamReader,
};

pub const CONTENT_TYPE: &str = "application/vnd.tangram.sync";

#[derive(Clone, Copy, Debug)]
pub struct Config {
	pub max_frame_size: u64,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_default")]
	pub ancestors: tg::node::AncestorsPull,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub eager: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub force: bool,

	#[serde_as(as = "CommaSeparatedString")]
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub get: Vec<tg::Referent<tg::Selector<tg::Id>>>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub group_children: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub metadata: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub organization_children: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub process_children: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub process_commands: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub process_errors: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub process_logs: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub process_outputs: bool,

	#[serde_as(as = "CommaSeparatedString")]
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub put: Vec<tg::Referent<tg::Id>>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub sandbox_processes: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub tag_targets: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub user_children: bool,
}

#[derive(
	Clone,
	Debug,
	derive_more::TryUnwrap,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Message {
	#[tangram_serialize(id = 2)]
	End,

	#[tangram_serialize(id = 0)]
	Get(GetMessage),

	#[tangram_serialize(id = 1)]
	Put(PutMessage),
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum GetMessage {
	#[tangram_serialize(id = 1)]
	Available(GetAvailableMessage),

	#[tangram_serialize(id = 3)]
	End,

	#[tangram_serialize(id = 0)]
	Node(GetNodeMessage),

	#[tangram_serialize(id = 2)]
	Progress(ProgressMessage),
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct GetNodeMessage {
	#[tangram_serialize(default = "return_true", id = 3, skip_serializing_if = "is_true")]
	pub descendants: bool,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_false")]
	pub eager: bool,

	#[tangram_serialize(id = 0)]
	pub selector: tg::Selector<tg::Id>,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub token: Option<tg::authorization::Token>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum GetAvailableMessage {
	#[tangram_serialize(id = 0)]
	Object(GetAvailableObjectMessage),

	#[tangram_serialize(id = 1)]
	Process(GetAvailableProcessMessage),
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct GetAvailableObjectMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::object::Id,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct GetAvailableProcessMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::process::Id,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_false")]
	pub node_command_available: bool,

	#[tangram_serialize(default, id = 8, skip_serializing_if = "is_false")]
	pub node_error_available: bool,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_false")]
	pub node_log_available: bool,

	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_false")]
	pub node_output_available: bool,

	#[tangram_serialize(default, id = 7, skip_serializing_if = "is_false")]
	pub subtree_available: bool,

	#[tangram_serialize(default, id = 4, skip_serializing_if = "is_false")]
	pub subtree_command_available: bool,

	#[tangram_serialize(default, id = 9, skip_serializing_if = "is_false")]
	pub subtree_error_available: bool,

	#[tangram_serialize(default, id = 5, skip_serializing_if = "is_false")]
	pub subtree_log_available: bool,

	#[tangram_serialize(default, id = 6, skip_serializing_if = "is_false")]
	pub subtree_output_available: bool,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum PutMessage {
	#[tangram_serialize(id = 3)]
	End,

	#[tangram_serialize(id = 1)]
	Missing(PutMissingMessage),

	#[tangram_serialize(id = 0)]
	Node(PutNodeMessage),

	#[tangram_serialize(id = 2)]
	Progress(ProgressMessage),
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
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

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct PutNodeGroupMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::group::Id,

	#[tangram_serialize(id = 1)]
	pub name: String,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::Id>,

	#[tangram_serialize(id = 3)]
	pub specifier: tg::Specifier,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct PutNodeObjectMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::object::Id,

	#[tangram_serialize(id = 1)]
	pub bytes: Bytes,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub metadata: Option<tg::object::Metadata>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct PutNodeOrganizationMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::organization::Id,

	#[tangram_serialize(id = 1)]
	pub name: String,

	#[tangram_serialize(id = 2)]
	pub specifier: tg::Specifier,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct PutNodeProcessMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::process::Id,

	#[tangram_serialize(id = 1)]
	pub bytes: Bytes,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub metadata: Option<tg::process::Metadata>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct PutNodeSandboxMessage {
	#[tangram_serialize(id = 0)]
	pub created_at: i64,

	#[tangram_serialize(id = 1)]
	pub data: tg::sandbox::get::Output,

	#[tangram_serialize(id = 2)]
	pub id: tg::sandbox::Id,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct PutNodeTagMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::tag::Id,

	#[tangram_serialize(id = 2)]
	pub name: String,

	#[tangram_serialize(default, id = 3, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::Id>,

	#[tangram_serialize(id = 4)]
	pub specifier: tg::Specifier,

	#[tangram_serialize(id = 1)]
	pub target: tg::Id,

	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub token: Option<tg::authorization::Token>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
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

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct PutMissingMessage {
	#[tangram_serialize(id = 0)]
	pub selector: tg::Selector<tg::Id>,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub token: Option<tg::authorization::Token>,
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ProgressMessage {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_default")]
	pub skipped: ProgressMessageAmounts,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_default")]
	pub transferred: ProgressMessageAmounts,
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ProgressMessageAmounts {
	#[tangram_serialize(default, id = 2, skip_serializing_if = "num::Zero::is_zero")]
	pub bytes: u64,

	#[tangram_serialize(default, id = 3, skip_serializing_if = "num::Zero::is_zero")]
	pub groups: u64,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "num::Zero::is_zero")]
	pub objects: u64,

	#[tangram_serialize(default, id = 4, skip_serializing_if = "num::Zero::is_zero")]
	pub organizations: u64,

	#[tangram_serialize(default, id = 0, skip_serializing_if = "num::Zero::is_zero")]
	pub processes: u64,

	#[tangram_serialize(default, id = 5, skip_serializing_if = "num::Zero::is_zero")]
	pub sandboxes: u64,

	#[tangram_serialize(default, id = 6, skip_serializing_if = "num::Zero::is_zero")]
	pub tags: u64,

	#[tangram_serialize(default, id = 7, skip_serializing_if = "num::Zero::is_zero")]
	pub users: u64,
}

impl tg::Session {
	pub async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + use<>> {
		let max_frame_size = self.client().sync.max_frame_size;
		let method = http::Method::POST;
		let (arg_in_body, uri) = match Uri::builder().path("/sync").query_params_strict(&arg) {
			Ok(builder) => (false, builder.build().unwrap()),
			Err(QueryParamsError::TooLarge) => {
				let uri = Uri::builder().path("/sync").build().unwrap();
				(true, uri)
			},
			Err(error) => return Err(tg::error!(!error, "failed to serialize the arg")),
		};

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
		let mut body = tangram_http::body::Boxed::with_stream(stream);
		if arg_in_body {
			body = tangram_http::body::arg::set(body, &arg)
				.map_err(|error| tg::error!(!error, "failed to add the sync arg"))?;
		}

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
		if arg_in_body {
			request = request.header(tangram_http::body::arg::HEADER, "true");
		}
		let request = request.body(body).unwrap();
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

		let reader =
			StreamReader::new(ReceiverStream::new(data_receiver).map_err(std::io::Error::other));
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

		let stream = stream::select(data_messages, trailer_messages).attach(task);

		Ok(stream)
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			max_frame_size: 64 * 1024 * 1024,
		}
	}
}
