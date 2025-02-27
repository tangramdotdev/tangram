use crate::{
	self as tg,
	util::serde::{CommaSeparatedString, is_false},
};
use bytes::Bytes;
use futures::{Stream, StreamExt as _, stream};
use num::ToPrimitive as _;
use serde_with::serde_as;
use std::pin::Pin;
use tangram_either::Either;
use tangram_futures::{read::Ext as _, write::Ext as _};
use tangram_http::{request::builder::Ext as _, response::Ext as _};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub commands: bool,

	pub items: Vec<Either<tg::process::Id, tg::object::Id>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub logs: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub outputs: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub recursive: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct QueryArg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub commands: bool,

	#[serde_as(as = "CommaSeparatedString")]
	items: Vec<Either<tg::process::Id, tg::object::Id>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub logs: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub outputs: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub recursive: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	remote: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Event {
	Complete(tg::export::Complete),
	Item(tg::export::Item),
	End,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Complete {
	Process(ProcessComplete),
	Object(ObjectComplete),
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ProcessComplete {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub commands_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub commands_weight: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,

	pub id: tg::process::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs_weight: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outputs_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outputs_weight: Option<u64>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ObjectComplete {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,

	pub id: tg::object::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub weight: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum Item {
	Process(ProcessItem),
	Object(ObjectItem),
}

#[derive(Debug, Clone)]
pub struct ProcessItem {
	pub id: tg::process::Id,
	pub data: tg::process::Data,
}

#[derive(Debug, Clone)]
pub struct ObjectItem {
	pub id: tg::object::Id,
	pub bytes: Bytes,
}

impl tg::Client {
	pub async fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Complete>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + use<>> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(QueryArg::from(arg)).unwrap();
		let uri = format!("/export?{query}");

		let sse = stream.map(|result| match result {
			Ok(event) => event.try_into(),
			Err(error) => error.try_into(),
		});

		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(
				http::header::ACCEPT,
				mime::APPLICATION_OCTET_STREAM.to_string(),
			)
			.header(
				http::header::CONTENT_TYPE,
				mime::TEXT_EVENT_STREAM.to_string(),
			)
			.sse(sse)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}

		let content_type = response
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		if !matches!(
			content_type
				.as_ref()
				.map(|content_type| (content_type.type_(), content_type.subtype())),
			Some((mime::APPLICATION, mime::OCTET_STREAM)),
		) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}
		let reader = response.reader();
		let stream = stream::try_unfold(reader, |mut reader| async move {
			let Some(item) = tg::export::Event::from_reader(&mut reader).await? else {
				return Ok(None);
			};
			Ok(Some((item, reader)))
		});

		Ok(stream)
	}
}

impl Event {
	pub async fn to_bytes(&self) -> Bytes {
		let mut bytes = Vec::new();
		self.to_writer(&mut bytes).await.unwrap();
		bytes.into()
	}

	pub async fn to_writer(&self, mut writer: impl AsyncWrite + Unpin + Send) -> tg::Result<()> {
		match self {
			Event::Complete(complete) => {
				writer
					.write_uvarint(0)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the tag"))?;

				let bytes = serde_json::to_vec(complete)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				writer
					.write_uvarint(bytes.len().to_u64().unwrap())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the event length"))?;
				writer
					.write_all(&bytes)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the event"))?;
			},

			Event::Item(item) => {
				writer
					.write_uvarint(1)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the tag"))?;
				item.to_writer(writer).await?;
			},

			Event::End => {
				writer
					.write_uvarint(2)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the tag"))?;
			},
		}
		Ok(())
	}

	pub async fn from_reader(
		mut reader: impl AsyncRead + Unpin + Send,
	) -> tg::Result<Option<Self>> {
		// Read the tag.
		let Some(tag) = reader
			.try_read_uvarint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the tag"))?
		else {
			return Ok(None);
		};

		let event = match tag {
			0 => {
				let len = reader
					.read_uvarint()
					.await
					.map_err(|source| tg::error!(!source, "failed to read the event length"))?
					.to_usize()
					.unwrap();
				let mut bytes = vec![0u8; len];
				reader
					.read_exact(&mut bytes)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the event"))?;
				let event = serde_json::from_slice(&bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Event::Complete(event)
			},

			1 => {
				let item = Item::from_reader(reader)
					.await?
					.ok_or_else(|| tg::error!("expected an item"))?;
				Event::Item(item)
			},

			2 => Event::End,

			_ => {
				return Err(tg::error!("invalid tag"));
			},
		};

		Ok(Some(event))
	}
}

impl Item {
	pub async fn to_bytes(&self) -> Bytes {
		let mut bytes = Vec::new();
		self.to_writer(&mut bytes).await.unwrap();
		bytes.into()
	}

	pub async fn to_writer(&self, mut writer: impl AsyncWrite + Unpin + Send) -> tg::Result<()> {
		match self {
			Item::Process(ProcessItem { id, data }) => {
				let id = id.to_string();
				writer
					.write_uvarint(id.len().to_u64().unwrap())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the id length"))?;
				writer
					.write_all(id.as_bytes())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the id"))?;
				let data = serde_json::to_vec(data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				writer
					.write_uvarint(data.len().to_u64().unwrap())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the data length"))?;
				writer
					.write_all(&data)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the data"))?;
			},

			Item::Object(ObjectItem { id, bytes }) => {
				let id = id.to_string();
				writer
					.write_uvarint(id.len().to_u64().unwrap())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the id length"))?;
				writer
					.write_all(id.as_bytes())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the id"))?;
				writer
					.write_uvarint(bytes.len().to_u64().unwrap())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the bytes length"))?;
				writer
					.write_all(bytes)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the bytes"))?;
			},
		}
		Ok(())
	}

	pub async fn from_reader(
		mut reader: impl AsyncRead + Unpin + Send,
	) -> tg::Result<Option<Self>> {
		// Read the ID.
		let Some(len) = reader
			.try_read_uvarint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the id length"))?
			.map(|value| value.to_usize().unwrap())
		else {
			return Ok(None);
		};
		let mut id = vec![0u8; len];
		reader
			.read_exact(&mut id)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the id"))?;
		let id = String::from_utf8(id)
			.map_err(|_| tg::error!("expected a string"))?
			.parse()
			.map_err(Either::into_inner)?;

		let item = match id {
			Either::Left(id) => {
				// Read the data.
				let len = reader
					.read_uvarint()
					.await
					.map_err(|source| tg::error!(!source, "failed to read the data length"))?
					.to_usize()
					.unwrap();
				let mut data = vec![0u8; len];
				reader
					.read_exact(&mut data)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the data"))?;
				let data = serde_json::from_slice(&data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;

				Item::Process(ProcessItem { id, data })
			},
			Either::Right(id) => {
				// Read the data.
				let len = reader
					.read_uvarint()
					.await
					.map_err(|source| tg::error!(!source, "failed to read the data length"))?
					.to_usize()
					.unwrap();
				let mut bytes = vec![0u8; len];
				reader
					.read_exact(&mut bytes)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the data"))?;
				let bytes = Bytes::from(bytes);

				Item::Object(ObjectItem { id, bytes })
			},
		};

		Ok(Some(item))
	}
}

impl From<Arg> for QueryArg {
	fn from(value: Arg) -> Self {
		Self {
			commands: value.commands,
			items: value.items,
			logs: value.logs,
			outputs: value.outputs,
			recursive: value.recursive,
			remote: value.remote,
		}
	}
}

impl From<QueryArg> for Arg {
	fn from(value: QueryArg) -> Self {
		Self {
			commands: value.commands,
			items: value.items,
			logs: value.logs,
			outputs: value.outputs,
			recursive: value.recursive,
			remote: value.remote,
		}
	}
}
