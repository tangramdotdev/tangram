#[derive(Clone, Copy)]
pub struct Arg<'a> {
	pub account: &'a crate::usage::Account,
	pub at: i64,
	pub sandbox_count: u64,
	pub usage: Option<&'a tangram_client::sandbox::Usage>,
}
