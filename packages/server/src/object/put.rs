use crate::Server;
use futures::FutureExt as _;
use num::ToPrimitive as _;
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;

impl Server {
	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<()> {
		self.put_object_inner(id, arg).boxed().await
	}

	async fn put_object_inner(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();

		let store_future = async {
			let arg = crate::store::PutArg {
				id: id.clone(),
				bytes: Some(arg.bytes.clone()),
				touched_at: now,
				cache_reference: None,
			};
			self.store.put(arg).await?;
			Ok::<_, tg::Error>(())
		};

		let messenger_future = async {
			let data = tg::object::Data::deserialize(id.kind(), arg.bytes.clone())?;
			let children = data.children();
			let size = arg.bytes.len().to_u64().unwrap();
			let message = crate::index::Message::PutObject(crate::index::PutObjectMessage {
				children,
				id: id.clone(),
				size,
				touched_at: now,
				cache_reference: None,
			});
			let message = serde_json::to_vec(&message)
				.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
			self.messenger
				.stream_publish("index".to_owned(), message.into())
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
			Ok::<_, tg::Error>(())
		};

		futures::try_join!(store_future, messenger_future)?;

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_put_object_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse::<tg::object::Id>()?;
		let bytes = request.bytes().await?;

		let found = tg::object::Id::new(id.kind(), &bytes);
		if id != found {
			let error = tg::error!(%expected = id, %found, "invalid object id");
			let response = http::Response::builder()
				.status(http::StatusCode::BAD_REQUEST)
				.bytes(serde_json::to_vec(&error).unwrap())
				.unwrap();
			return Ok(response);
		}

		let arg = tg::object::put::Arg { bytes };
		handle.put_object(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
