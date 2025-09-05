use crate::Row;

pub trait Serialize {
	fn serialize(&self) -> Result<Row, Box<dyn std::error::Error + Send + Sync + 'static>>;
}
