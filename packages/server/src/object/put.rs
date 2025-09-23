use {
	crate::Server,
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client as tg,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
};

impl Server {
	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();

		let put_arg = crate::store::PutArg {
			id: id.clone(),
			bytes: Some(arg.bytes.clone()),
			touched_at: now,
			cache_reference: None,
		};
		self.store.put(put_arg).await?;

		let data = tg::object::Data::deserialize(id.kind(), arg.bytes.clone())?;
		let mut children = BTreeSet::new();
		data.children(&mut children);
		let size = arg.bytes.len().to_u64().unwrap();
		let message = crate::index::Message::PutObject(crate::index::message::PutObject {
			cache_entry: None,
			children,
			complete: false,
			id: id.clone(),
			metadata: tg::object::Metadata::default(),
			size,
			touched_at: now,
		});
		let message = message.serialize()?;
		let _published = self
			.messenger
			.stream_publish("index".to_owned(), message)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

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

		let actual = tg::object::Id::new(id.kind(), &bytes);
		if id != actual {
			let error = tg::error!(%expected = id, %actual, "invalid object id");
			let response = http::Response::builder()
				.status(http::StatusCode::BAD_REQUEST)
				.bytes(serde_json::to_vec(&error.to_data()).unwrap())
				.unwrap();
			return Ok(response);
		}

		let arg = tg::object::put::Arg { bytes };
		handle.put_object(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
