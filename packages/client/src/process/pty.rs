use crate as tg;

pub mod get;
pub mod put;

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Pty {
	pub rows: u64,
	pub columns: u64,
}
