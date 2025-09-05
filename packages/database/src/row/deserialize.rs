use crate::Row;

pub trait Deserialize: Sized {
	fn deserialize(row: Row) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>>;
}
