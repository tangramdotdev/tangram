use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::tag::Id,
	pub item: tg::Either<tg::object::Id, tg::process::Id>,
	pub name: String,
	pub parent: Option<tg::Id>,
	pub specifier: tg::Specifier,
}
