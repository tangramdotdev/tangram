pub mod get;
pub mod put;

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Size {
	pub rows: u16,
	pub cols: u16,
}
