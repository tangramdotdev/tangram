use crate as tg;

pub struct Output {
	error: Option<tg::Error>,
	exit: Option<tg::process::Exit>,
	output: Option<tg::Value>,
	status: tg::process::Status,
}
