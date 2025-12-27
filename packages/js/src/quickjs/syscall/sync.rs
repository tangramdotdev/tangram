use {
	super::Result,
	crate::quickjs::{StateHandle, serde::Serde},
	futures::{StreamExt as _, stream},
	rquickjs as qjs,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
};

#[derive(serde::Deserialize)]
pub struct SyncItem {
	pub id: tg::object::Id,
	pub data: tg::object::Data,
}

pub async fn sync(ctx: qjs::Ctx<'_>, items: Serde<Vec<SyncItem>>) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(items) = items;
	let result = async {
		if items.is_empty() {
			return Ok(());
		}
		state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let arg = tg::sync::Arg::default();
					let mut messages = Vec::new();
					let message = tg::sync::Message::Get(tg::sync::GetMessage::End);
					messages.push(Ok(message));
					for item in items {
						let id = item.id;
						let data = item.data;
						let bytes = data.serialize()?;
						let message = tg::sync::Message::Put(tg::sync::PutMessage::Item(
							tg::sync::PutItemMessage::Object(tg::sync::PutItemObjectMessage {
								id,
								bytes,
							}),
						));
						messages.push(Ok(message));
					}
					let message = tg::sync::Message::Put(tg::sync::PutMessage::End);
					messages.push(Ok(message));
					let stream = stream::iter(messages).boxed();
					let stream = handle.sync(arg, stream).await?.boxed();
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
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to import"))?;
		Ok(())
	}
	.await;
	Result(result)
}
