#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub object_count: u64,
	pub object_size: u64,
	pub process_count: u64,
}
