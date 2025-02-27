use crate::Server;
use tangram_client as tg;

impl Server {
	pub async fn store_objects(&self, _value: &tg::Value) -> tg::Result<()> {
		// store all objects in a single transaction.
		todo!()
	}
}
