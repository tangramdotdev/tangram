use super::State;
use futures::{StreamExt as _, stream};
use num::ToPrimitive as _;
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
	let (Serde(items),) = args;
	if items.is_empty() {
		return Ok(());
	}
	let server = state.server.clone();
	state
		.main_runtime_handle
		.spawn({
			async move {
				let arg = tg::import::Arg::default();
				let stream = items.into_iter().map(|item| {
					let id = item.id;
					let data = item.data;
					let size = data.serialize()?.len().to_u64().unwrap();
					let item = tg::export::Item::Object(tg::export::ObjectItem { id, data, size });
					let event = tg::export::Event::Item(item);
					Ok::<_, tg::Error>(event)
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
