use super::State;
use futures::{StreamExt as _, stream};
use std::{pin::pin, rc::Rc};
use tangram_client as tg;
use tangram_futures::stream::TryExt as _;
use tangram_v8::Serde;

#[derive(serde::Deserialize)]
pub struct Item {
	id: tg::object::Id,
	data: tg::object::Data,
}

pub async fn import(state: Rc<State>, args: (Serde<Vec<Item>>,)) -> tg::Result<()> {
	let (Serde(stream),) = args;
	let server = state.server.clone();
	state
		.main_runtime_handle
		.spawn({
			async move {
				let arg = tg::import::Arg {
					items: vec![],
					remote: None,
				};
				let stream = stream.into_iter().map(|item| {
					let id = item.id;
					let bytes = item.data.serialize()?;
					let item = tg::export::Item::Object(tg::export::ObjectItem { id, bytes });
					Ok::<_, tg::Error>(item)
				});
				let stream = stream::iter(stream).boxed();
				let stream = server.import(arg, stream).await?;
				pin!(stream)
					.try_last()
					.await?
					.ok_or_else(|| tg::error!("expected an event"))?
					.try_unwrap_end()
					.ok()
					.ok_or_else(|| tg::error!("expected the end"))?;
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to import"))?;
	Ok(())
}
