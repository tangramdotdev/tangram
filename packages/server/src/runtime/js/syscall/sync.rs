use {
	super::State,
	futures::{StreamExt as _, stream},
	std::{pin::pin, rc::Rc},
	tangram_client as tg,
	tangram_futures::stream::TryExt as _,
	tangram_v8::Serde,
};

#[derive(serde::Deserialize)]
pub struct Item {
	id: tg::object::Id,
	data: tg::object::Data,
}

pub async fn sync(state: Rc<State>, args: (Serde<Vec<Item>>,)) -> tg::Result<()> {
	let (Serde(items),) = args;
	if items.is_empty() {
		return Ok(());
	}
	let handle = state.handle.clone();
	state
		.main_runtime_handle
		.spawn({
			async move {
				let arg = tg::sync::Arg::default();
				let mut messages = Vec::new();
				messages.push(Ok(tg::sync::Message::Get(None)));
				for item in items {
					let id = item.id;
					let data = item.data;
					let bytes = data.serialize()?;
					let message = tg::sync::Message::Put(Some(tg::sync::PutMessage::Object(
						tg::sync::ObjectPutMessage { id, bytes },
					)));
					messages.push(Ok(message));
				}
				messages.push(Ok(tg::sync::Message::Put(None)));
				let stream = stream::iter(messages).boxed();
				let stream = handle.sync(arg, stream).await?;
				pin!(stream)
					.try_last()
					.await?
					.ok_or_else(|| tg::error!("expected a message"))?
					.try_unwrap_end()
					.ok()
					.ok_or_else(|| tg::error!("expected the end message"))?;
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to import"))?;
	Ok(())
}
