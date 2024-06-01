use crate::Server;
use futures::{stream, Stream, StreamExt as _};
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn push_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::build::push::Event>> + Send + 'static> {
		Ok(stream::empty())
	}
}

impl Server {
	pub(crate) async fn handle_push_build_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let stream = handle.push_build(&id, arg).await?;
		let sse = stream.map(|result| match result {
			Ok(tg::build::push::Event::Progress(progress)) => {
				let data = serde_json::to_string(&progress).unwrap();
				let event = tangram_http::sse::Event {
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Ok(tg::build::push::Event::End) => {
				let event = "end".to_owned();
				let event = tangram_http::sse::Event {
					event: Some(event),
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Err(error) => {
				let data = serde_json::to_string(&error).unwrap();
				let event = "error".to_owned();
				let event = tangram_http::sse::Event {
					data,
					event: Some(event),
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
		});
		let body = Outgoing::sse(sse);
		let response = http::Response::builder().ok().body(body).unwrap();
		Ok(response)
	}
}

// pub async fn push<H1, H2>(&self, handle: &H1, remote: &H2) -> tg::Result<()>
// where
// 	H1: tg::Handle,
// 	H2: tg::Handle,
// {
// 	let output = handle.get_build(&self.id).await?;
// 	let arg = tg::build::children::Arg {
// 		timeout: Some(std::time::Duration::ZERO),
// 		..Default::default()
// 	};
// 	let children = handle
// 		.get_build_children(&self.id, arg)
// 		.await?
// 		.map_ok(|chunk| stream::iter(chunk.items).map(Ok::<_, tg::Error>))
// 		.try_flatten()
// 		.try_collect()
// 		.await?;
// 	let arg = tg::build::put::Arg {
// 		id: output.id,
// 		children,
// 		host: output.host,
// 		log: output.log,
// 		outcome: output.outcome,
// 		retry: output.retry,
// 		status: output.status,
// 		target: output.target,
// 		created_at: output.created_at,
// 		dequeued_at: output.dequeued_at,
// 		started_at: output.started_at,
// 		finished_at: output.finished_at,
// 	};
// 	arg.children
// 		.iter()
// 		.cloned()
// 		.map(Self::with_id)
// 		.map(|build| async move { build.push(handle, remote).await })
// 		.collect::<FuturesUnordered<_>>()
// 		.try_collect()
// 		.await?;
// 	arg.objects()
// 		.iter()
// 		.cloned()
// 		.map(tg::object::Handle::with_id)
// 		.map(|object| async move { object.push(handle, remote).await })
// 		.collect::<FuturesUnordered<_>>()
// 		.try_collect()
// 		.await?;
// 	remote
// 		.put_build(&self.id, arg)
// 		.await
// 		.map_err(|source| tg::error!(!source, "failed to put the object"))?;
// 	Ok(())
// }
