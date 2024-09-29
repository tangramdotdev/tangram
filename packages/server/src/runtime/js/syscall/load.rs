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
