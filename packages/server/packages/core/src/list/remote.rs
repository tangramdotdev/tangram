use tangram_client::prelude::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Query {
	List(tg::list::Arg),
	Match(tg::match_::Arg),
}
