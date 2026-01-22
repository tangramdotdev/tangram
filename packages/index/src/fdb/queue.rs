use {super::Index, tangram_client::prelude::*};

impl Index {
	pub async fn queue(&self, _batch_size: usize) -> tg::Result<usize> {
		Ok(0)
	}
}
