use tangram_client::prelude::*;

#[derive(Debug)]
pub enum Key<'a> {
	Object(&'a tg::object::Id),
}
