use {super::Index, crate::CleanOutput, tangram_client::prelude::*};

impl Index {
	pub async fn clean(&self, _max_touched_at: i64, _n: usize) -> tg::Result<CleanOutput> {
		Ok(CleanOutput::default())
	}
}
