use {super::State, std::rc::Rc, tangram_client::prelude::*, tangram_v8::Serde};

#[derive(serde::Deserialize)]
pub struct Arg {
	objects: Vec<Object>,
}

#[derive(serde::Deserialize)]
pub struct Object {
	id: tg::object::Id,
	data: tg::object::Data,
}

pub async fn object_batch(state: Rc<State>, args: (Serde<Arg>,)) -> tg::Result<()> {
	let (Serde(arg),) = args;
	if arg.objects.is_empty() {
		return Ok(());
	}
	let handle = state.handle.clone();
	state
		.main_runtime_handle
		.spawn({
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
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to post object batch"))?;
	Ok(())
}
