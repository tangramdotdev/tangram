#[derive(Clone, Copy)]
pub struct Arg<'a> {
	pub account: &'a crate::usage::Account,
	pub at: i64,
	pub cpu: Option<u64>,
	pub cpu_dedicated: Option<u64>,
	pub memory: Option<u64>,
	pub sandbox_count: u64,
}
