use super::State;
use std::rc::Rc;
use tangram_client as tg;

pub async fn load(state: Rc<State>, args: (tg::object::Id,)) -> tg::Result<tg::object::Object> {
	let (id,) = args;
	let handle = &tg::object::Handle::with_id(id);
	let object = handle.object(&state.server).await?;
	Ok(object)
}
