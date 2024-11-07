use super::State;
use std::rc::Rc;
use tangram_client as tg;

pub async fn load(state: Rc<State>, args: (tg::object::Id,)) -> tg::Result<tg::object::Object> {
	let (id,) = args;
	let server = state.server.clone();
	let object = state
		.main_runtime_handle
		.spawn(async move { tg::object::Handle::with_id(id).object(&server).await })
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to load the object"))?;
	Ok(object)
}

pub async fn store(state: Rc<State>, args: (tg::object::Object,)) -> tg::Result<tg::object::Id> {
	let (object,) = args;
	let server = state.server.clone();
	let id = state
		.main_runtime_handle
		.spawn(async move { tg::object::Handle::with_object(object).id(&server).await })
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to store the object"))?;
	Ok(id)
}
