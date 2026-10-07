use tangram_client::prelude::*;

pub mod archive;
pub mod capacity;
pub mod index;
pub mod log;
pub mod object;
pub mod prelude;

pub trait Cache: archive::Cache + index::Cache + log::Cache + object::Cache {
	fn flush(&self) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_capacity(
		&self,
	) -> impl Future<Output = tg::Result<Option<capacity::Capacity>>> + Send;
}
