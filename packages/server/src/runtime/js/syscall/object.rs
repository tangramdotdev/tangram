use super::State;
use std::rc::Rc;
use tangram_client::{self as tg, prelude::*};
use tangram_v8::Serde;

pub async fn get(
	state: Rc<State>,
	args: (Serde<tg::object::Id>,),
) -> tg::Result<Serde<tg::object::Data>> {
	let (Serde(id),) = args;
	let server = state.server.clone();
	let data = state
		.main_runtime_handle
		.spawn({
			let id = id.clone();
			async move {
				let tg::object::get::Output { bytes } = server.get_object(&id).await?;
				let data = tg::object::Data::deserialize(id.kind(), bytes)?;
				Ok::<_, tg::Error>(data)
			}
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
	Ok(Serde(data))
}

pub fn id(
	_state: Rc<State>,
	_scope: &mut v8::HandleScope,
	args: (Serde<tg::object::Data>,),
) -> tg::Result<Serde<tg::object::Id>> {
	let (Serde(data),) = args;
	let bytes = data.serialize()?;
	let id = tg::object::Id::new(data.kind(), &bytes);
	Ok(Serde(id))
}
