use crate::Server;
use tangram_client as tg;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub complete: bool,
	pub commands_complete: bool,
	pub outputs_complete: bool,
}

impl Server {
	pub(crate) async fn try_get_process_complete(
		&self,
		_id: &tg::process::Id,
	) -> tg::Result<Option<Output>> {
		todo!()
	}

	pub(crate) async fn try_get_process_complete_batch(
		&self,
		_ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		todo!()
	}
}
