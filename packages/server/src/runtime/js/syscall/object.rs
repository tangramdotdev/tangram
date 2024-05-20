use super::State;
use std::rc::Rc;
use tangram_client as tg;

pub async fn load(state: Rc<State>, args: (tg::object::Id,)) -> tg::Result<tg::object::Object> {
	let (id,) = args;
	tg::object::Handle::with_id(id).object(&state.server).await
}

pub async fn store(state: Rc<State>, args: (tg::object::Object,)) -> tg::Result<tg::object::Id> {
	let (object,) = args;
	let handle = tg::object::Handle::with_object(object);
	let id = handle.id(&state.server, None).await?;
	Ok(id.clone())
}