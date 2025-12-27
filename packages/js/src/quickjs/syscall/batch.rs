use {
	super::Result,
	crate::quickjs::{StateHandle, serde::Serde},
	rquickjs as qjs,
	tangram_client::prelude::*,
};

#[derive(serde::Deserialize)]
pub struct Arg {
	pub objects: Vec<Object>,
}

#[derive(serde::Deserialize)]
pub struct Object {
	pub id: tg::object::Id,
	pub data: tg::object::Data,
}

pub async fn object_batch(ctx: qjs::Ctx<'_>, arg: Serde<Arg>) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(arg) = arg;
	let result = async {
		if arg.objects.is_empty() {
			return Ok(());
		}
		state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let mut batch_objects = Vec::with_capacity(arg.objects.len());
					for object in arg.objects {
						let bytes = object.data.serialize()?;
						batch_objects.push(tg::object::batch::Object {
							id: object.id,
							bytes,
						});
					}
					let arg = tg::object::batch::Arg {
						objects: batch_objects,
						..Default::default()
					};
					handle.post_object_batch(arg).await?;
					Ok::<_, tg::Error>(())
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to post object batch"))?;
		Ok(())
	}
	.await;
	Result(result)
}
