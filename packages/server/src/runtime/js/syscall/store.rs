use super::State;
use std::rc::Rc;
use tangram_client as tg;

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
