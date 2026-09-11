use {super::Cache, tangram_client::prelude::*};

impl Cache {
	pub(super) async fn flush(&self) -> tg::Result<()> {
		Ok(())
	}
}
