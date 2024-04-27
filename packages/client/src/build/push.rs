use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};
use futures::{
	stream::{self, FuturesUnordered},
	TryStreamExt as _,
};
use tokio_stream::StreamExt as _;

impl tg::Build {
	pub async fn push<H1, H2>(&self, handle: &H1, remote: &H2) -> tg::Result<()>
	where
		H1: tg::Handle,
		H2: tg::Handle,
	{
		let output = handle.get_build(&self.id).await?;
		let arg = tg::build::children::Arg {
			timeout: Some(std::time::Duration::ZERO),
			..Default::default()
		};
		let children = handle
			.get_build_children(&self.id, arg, None)
			.await?
			.map_ok(|chunk| stream::iter(chunk.items).map(Ok::<_, tg::Error>))
			.try_flatten()
			.try_collect()
			.await?;
		let arg = tg::build::put::Arg {
			id: output.id,
			children,
			count: output.count,
			host: output.host,
			log: output.log,
			outcome: output.outcome,
			retry: output.retry,
			status: output.status,
			target: output.target,
			weight: output.weight,
			created_at: output.created_at,
			dequeued_at: output.dequeued_at,
			started_at: output.started_at,
			finished_at: output.finished_at,
		};
		arg.children
			.iter()
			.cloned()
			.map(Self::with_id)
			.map(|build| async move { build.push(handle, remote).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		arg.objects()
			.iter()
			.cloned()
			.map(tg::object::Handle::with_id)
			.map(|object| async move { object.push(handle, remote, None).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		remote
			.put_build(&self.id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;
		Ok(())
	}
}

impl tg::Client {
	pub async fn push_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/push");
		let body = Outgoing::empty();
		let mut request = http::request::Builder::default().method(method).uri(uri);
		if let Some(token) = self.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let request = request.body(body).unwrap();
		let response = self.send(request).await?;
		response.success().await?;
		Ok(())
	}
}
