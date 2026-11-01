use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub after: Option<Position>,
	pub limit: usize,
	pub principal: tg::Principal,
	pub status: Option<tg::sandbox::Status>,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub data: Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>,
	pub next: Option<Position>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Position {
	pub sandbox: tg::sandbox::Id,
	pub status: tg::sandbox::Status,
}
