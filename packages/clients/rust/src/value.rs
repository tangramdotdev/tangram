pub use self::{
	data::Data,
	handle::{Array, Map, Value as Handle},
	parse::parse,
};

pub mod data;
pub mod handle;
pub mod load;
pub mod parse;
pub mod print;

#[derive(Clone, Copy, Debug)]
pub struct Config {
	pub load_concurrency: usize,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			load_concurrency: 16,
		}
	}
}
