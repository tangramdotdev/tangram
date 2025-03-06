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
		let store_future = async {
			self.store.put(id, arg.bytes.clone()).await?;
			Ok::<_, tg::Error>(())
		};

		let messenger_future = async {
			let data = tg::object::Data::deserialize(id.kind(), &arg.bytes)?;
			let children = data.children();
			let message = crate::index::Message {
				id: id.clone(),
				size: arg.bytes.len().to_u64().unwrap(),
				children,
			};
			let message = serde_json::to_vec(&message)
				.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
			self.messenger
				.publish("index".to_owned(), message.into())
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
		let id = id.parse()?;
		let bytes = request.bytes().await?;
		let arg = tg::object::put::Arg { bytes };
		handle.put_object(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
