use tangram_client as tg;

pub struct Arg {
	pub account: tg::usage::Account,
	pub email: Option<String>,
	pub name: String,
}
