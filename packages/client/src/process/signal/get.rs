#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Event {
	Signal(i32),
	End,
}
