use {
	crate::prelude::*,
	bytes::Bytes,
	futures::{prelude::*, stream::BoxStream},
	http_body_util::BodyStream,
	num::ToPrimitive as _,
	serde_with::{DisplayFromStr, PickFirst, serde_as},
	tangram_either::Either,
	tangram_futures::{read::Ext, stream::Ext as _, task::Task, write::Ext as _},
	tangram_http::{Body, response::Ext as _},
	tangram_util::serde::{CommaSeparatedString, is_false},
	tokio::io::AsyncReadExt as _,
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::io::StreamReader,
};

pub const CONTENT_TYPE: &str = "application/vnd.tangram.sync";

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub commands: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub eager: bool,

	#[serde_as(as = "CommaSeparatedString")]
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub get: Vec<Either<tg::object::Id, tg::process::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub logs: bool,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub outputs: bool,

	#[serde_as(as = "CommaSeparatedString")]
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub put: Vec<Either<tg::object::Id, tg::process::Id>>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub recursive: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,
}

#[derive(
	Debug,
	Clone,
	derive_more::TryUnwrap,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Message {
	#[tangram_serialize(id = 0)]
	Get(GetMessage),

	#[tangram_serialize(id = 1)]
	Put(PutMessage),

	#[tangram_serialize(id = 2)]
	End,
}

#[derive(Debug, Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum GetMessage {
	#[tangram_serialize(id = 0)]
	Item(GetItemMessage),

	#[tangram_serialize(id = 1)]
	Stored(GetStoredMessage),

	#[tangram_serialize(id = 2)]
	Progress(ProgressMessage),

	#[tangram_serialize(id = 3)]
	End,
}

#[derive(Debug, Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum GetItemMessage {
	#[tangram_serialize(id = 0)]
	Object(GetItemObjectMessage),

	#[tangram_serialize(id = 1)]
	Process(GetItemProcessMessage),
}

#[derive(Debug, Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct GetItemObjectMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::object::Id,

	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_false")]
	pub eager: bool,
}

#[derive(Debug, Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct GetItemProcessMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::process::Id,

	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_false")]
	pub eager: bool,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum GetStoredMessage {
	#[tangram_serialize(id = 0)]
	Object(GetStoredObjectMessage),

	#[tangram_serialize(id = 1)]
	Process(GetStoredProcessMessage),
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct GetStoredObjectMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::object::Id,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct GetStoredProcessMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::process::Id,

	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_false")]
	pub node_command_stored: bool,

	#[tangram_serialize(id = 6, default, skip_serializing_if = "is_false")]
	pub node_log_stored: bool,

	#[tangram_serialize(id = 2, default, skip_serializing_if = "is_false")]
	pub node_output_stored: bool,

	#[tangram_serialize(id = 3, default, skip_serializing_if = "is_false")]
	pub subtree_stored: bool,

	#[tangram_serialize(id = 4, default, skip_serializing_if = "is_false")]
	pub subtree_command_stored: bool,

	#[tangram_serialize(id = 7, default, skip_serializing_if = "is_false")]
	pub subtree_log_stored: bool,

	#[tangram_serialize(id = 5, default, skip_serializing_if = "is_false")]
	pub subtree_output_stored: bool,
}

#[derive(Debug, Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum PutMessage {
	#[tangram_serialize(id = 0)]
	Item(PutItemMessage),

	#[tangram_serialize(id = 1)]
	Missing(PutMissingMessage),

	#[tangram_serialize(id = 2)]
	Progress(ProgressMessage),

	#[tangram_serialize(id = 3)]
	End,
}

#[derive(Debug, Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum PutItemMessage {
	#[tangram_serialize(id = 0)]
	Object(PutItemObjectMessage),

	#[tangram_serialize(id = 1)]
	Process(PutItemProcessMessage),
}

#[derive(Debug, Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct PutItemObjectMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::object::Id,

	#[tangram_serialize(id = 1)]
	pub bytes: Bytes,
}

#[derive(Debug, Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct PutItemProcessMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::process::Id,

	#[tangram_serialize(id = 1)]
	pub bytes: Bytes,
}

#[derive(Debug, Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum PutMissingMessage {
	#[tangram_serialize(id = 0)]
	Object(PutMissingObjectMessage),

	#[tangram_serialize(id = 1)]
	Process(PutMissingProcessMessage),
}

#[derive(Debug, Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct PutMissingObjectMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::object::Id,
}

#[derive(Debug, Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct PutMissingProcessMessage {
	#[tangram_serialize(id = 0)]
	pub id: tg::process::Id,
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
	#[tangram_serialize(id = 0, default, skip_serializing_if = "num::Zero::is_zero")]
	pub processes: u64,

	#[tangram_serialize(id = 1, default, skip_serializing_if = "num::Zero::is_zero")]
	pub objects: u64,

	#[tangram_serialize(id = 2, default, skip_serializing_if = "num::Zero::is_zero")]
	pub bytes: u64,
}

impl tg::Client {
	pub async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + use<>> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/sync?{query}");

		// Create the body.
		let body = Body::with_stream(stream.then(|result| async {
			let frame = match result {
				Ok(message) => {
					let message = tangram_serialize::to_vec(&message).unwrap();
					let mut bytes = Vec::with_capacity(9 + message.len());
					bytes
						.write_uvarint(message.len().to_u64().unwrap())
						.await
						.unwrap();
					bytes.write_all(&message).await.unwrap();
					hyper::body::Frame::data(bytes.into())
				},
				Err(error) => {
					let mut trailers = http::HeaderMap::new();
					trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
					let json = serde_json::to_string(&error.to_data()).unwrap();
					trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
					hyper::body::Frame::trailers(trailers)
				},
			};
			Ok::<_, tg::Error>(frame)
		}));

		// Send the request.
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, tg::sync::CONTENT_TYPE.to_string())
			.header(
				http::header::CONTENT_TYPE,
				tg::sync::CONTENT_TYPE.to_string(),
			)
			.body(body)
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
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
		let data_messages = stream::try_unfold(reader, |mut reader| async move {
			let Some(len) = reader
				.try_read_uvarint()
				.await
				.map_err(|source| tg::error!(!source, "failed to read the length"))?
				.map(|value| value.to_usize().unwrap())
			else {
				return Ok(None);
			};
			let mut bytes = vec![0; len];
			reader
				.read_exact(&mut bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the message"))?;
			let message = tangram_serialize::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the message"))?;
			Ok(Some((message, reader)))
		});

		let trailers = ReceiverStream::new(trailer_receiver);
		let trailer_messages = trailers.then(|trailers| async move {
			let event = trailers
				.get("x-tg-event")
				.ok_or_else(|| tg::error!("missing event"))?
				.to_str()
				.map_err(|source| tg::error!(!source, "invalid event"))?;
			if let "error" = event {
				let data = trailers
					.get("x-tg-data")
					.ok_or_else(|| tg::error!("missing data"))?
					.to_str()
					.map_err(|source| tg::error!(!source, "invalid data"))?;
				let error = serde_json::from_str(data).map_err(|source| {
					tg::error!(!source, "failed to deserialize the header value")
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
