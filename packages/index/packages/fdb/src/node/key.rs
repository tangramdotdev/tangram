use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Node(tg::Specifier),
}
