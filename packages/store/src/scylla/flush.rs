use {super::Store, tangram_client::prelude::*};

impl Store {
	pub(super) async fn flush(&self) -> tg::Result<()> {
		Ok(())
	}
}
