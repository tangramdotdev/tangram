use crate::Server;
use tangram_client as tg;

impl Server {
	pub(crate) async fn try_create_process_token(
		&self,
		_process: &tg::process::Id,
	) -> tg::Result<Option<String>> {
		// TODO
		return Ok(Some("token".into()));
	}

	pub(crate) async fn check_process_token(
		&self,
		_process: &tg::process::Id,
		_token: &str,
	) -> tg::Result<bool> {
		// TODO
		return Ok(true);
	}
}
