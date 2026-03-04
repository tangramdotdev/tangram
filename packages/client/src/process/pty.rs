use crate as tg;

#[derive(
	Copy,
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Pty {
	#[tangram_serialize(id = 0)]
	pub size: tg::process::pty::Size,
}

#[derive(
	Copy,
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Size {
	#[tangram_serialize(id = 0)]
	pub rows: u16,
	#[tangram_serialize(id = 1)]
	pub cols: u16,
}
