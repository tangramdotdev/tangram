use {crate::Session, tangram_client::prelude::*};

impl Session {
	#[expect(
		dead_code,
		reason = "Authorization enforcement will call this method once it is wired into the server."
	)]
	pub(crate) async fn authorize(
		&self,
		_id: tg::Id,
		_permission: tg::grant::Permission,
	) -> tg::Result<bool> {
		Ok(true)
	}
}
