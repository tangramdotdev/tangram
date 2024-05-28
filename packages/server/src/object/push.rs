use crate::Server;
use futures::{stream, Stream, TryStreamExt as _};
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn push_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::Progress>> + Send + 'static> {
		Ok(stream::empty())
	}
}

impl Server {
	pub(crate) async fn handle_push_object_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let stream = handle.push_object(&id, arg).await?;
		let sse = stream.map_ok(|event| {
			let data = serde_json::to_string(&event).unwrap();
			tangram_http::sse::Event::with_data(data)
		});
		let body = Outgoing::sse(sse);
		let response = http::Response::builder().ok().body(body).unwrap();
		Ok(response)
	}
}

// impl tg::Object {
// 	pub async fn push<H1, H2>(
// 		&self,
// 		handle: &H1,
// 		remote: &H2,
// 		transaction: Option<&H2::Transaction<'_>>,
// 	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::Progress>>>
// 	where
// 		H1: crate::Handle,
// 		H2: crate::Handle,
// 	{
// 		let id = self.id(handle, None).await?;
// 		let data = self.data(handle, None).await?;
// 		let bytes = data.serialize()?;
// 		let arg = tg::object::put::Arg { bytes };
// 		let output = remote
// 			.put_object(&id.clone(), arg, transaction)
// 			.boxed()
// 			.await
// 			.map_err(|source| tg::error!(!source, "failed to put the object"))?;
// 		output
// 			.incomplete
// 			.into_iter()
// 			.map(Self::with_id)
// 			.map(|object| async move { object.push(handle, remote, transaction).await })
// 			.collect::<FuturesUnordered<_>>()
// 			.try_collect()
// 			.await?;
// 		Ok(())
// 	}
// }
