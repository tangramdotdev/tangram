use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub after: Option<Position>,
	pub limit: usize,
	pub principal: tg::Principal,
	pub status: Option<tg::process::Status>,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub data: Vec<(tg::process::Id, crate::process::Process, tg::process::Data)>,
	pub next: Option<Position>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Position {
	pub process: tg::process::Id,
	pub status: tg::process::Status,
}
