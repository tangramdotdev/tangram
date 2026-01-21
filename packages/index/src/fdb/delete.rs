use {super::Index, tangram_client::prelude::*};

impl Index {
	pub async fn delete_tags(&self, _tags: &[String]) -> tg::Result<()> {
		todo!()
	}
}
