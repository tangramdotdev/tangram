use {crate::Session, tangram_client::prelude::*};

#[derive(Clone, Debug)]
#[expect(
	dead_code,
	reason = "Authorization enforcement will read the process grant fields once it is wired in."
)]
pub(crate) struct ProcessGrant {
	pub created_at: i64,
	pub expires_at: i64,
	pub node: bool,
	pub node_command: bool,
	pub node_error: bool,
	pub node_log: bool,
	pub node_output: bool,
	pub principal: tg::Principal,
	pub process: tg::process::Id,
	pub subtree: bool,
	pub subtree_command: bool,
	pub subtree_error: bool,
	pub subtree_log: bool,
	pub subtree_output: bool,
}

impl Session {
	pub(crate) fn authorize_object(
		_id: &tg::object::Id,
		_principal: Option<&tg::Principal>,
		_grants: &[tangram_object_store::Grant],
	) -> bool {
		true
	}

	pub(crate) fn authorize_process(
		_id: &tg::process::Id,
		_principal: Option<&tg::Principal>,
		_grants: &[ProcessGrant],
	) -> bool {
		true
	}

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
