use {crate::Session, tangram_client::prelude::*, tangram_index::prelude::*};

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
		&self,
		_id: &tg::object::Id,
		_grants: &[tangram_object_store::Grant],
	) -> bool {
		let _principal = self.context.principal.as_ref();
		true
	}

	pub(crate) fn authorize_process(
		&self,
		_id: &tg::process::Id,
		_grants: &[ProcessGrant],
	) -> bool {
		let _principal = self.context.principal.as_ref();
		true
	}

	pub(crate) async fn authorize(
		&self,
		resource: tg::grant::Resource,
		permission: tg::grant::Permission,
	) -> tg::Result<Option<bool>> {
		self.server
			.index
			.authorize(resource, permission, self.context.principal.as_ref())
			.await
	}

	/// Compute the permissions the current principal has on a tag item, to be recorded on the tag.
	pub(crate) async fn recorded_tag_permissions(
		&self,
		item: &tg::tag::data::Item,
	) -> tg::Result<Vec<tg::grant::Permission>> {
		let (resource, aspects): (tg::Id, Vec<tg::grant::Permission>) = match item {
			tg::tag::data::Item::Object(id) => (
				id.clone().into(),
				vec![tg::grant::Permission::Object(
					tg::grant::permission::object::Permission::Node,
				)],
			),
			tg::tag::data::Item::Process(id) => (
				id.clone().into(),
				[
					tg::grant::permission::process::Permission::Node,
					tg::grant::permission::process::Permission::NodeCommand,
					tg::grant::permission::process::Permission::NodeError,
					tg::grant::permission::process::Permission::NodeLog,
					tg::grant::permission::process::Permission::NodeOutput,
				]
				.into_iter()
				.map(tg::grant::Permission::Process)
				.collect(),
			),
		};
		// Root is always authorized, so it records the subtree variant of every aspect without consulting the index.
		if matches!(self.context.principal, Some(tg::Principal::Root)) {
			return Ok(aspects
				.into_iter()
				.map(tg::grant::Permission::subtree)
				.collect());
		}
		// For each aspect, record the strongest permission the principal has, trying the subtree variant before the node variant.
		let mut permissions = Vec::new();
		for aspect in aspects {
			for permission in [aspect.subtree(), aspect] {
				let resource = tg::grant::Resource::Id(resource.clone());
				if self.authorize(resource, permission).await? == Some(true) {
					permissions.push(permission);
					break;
				}
			}
		}
		Ok(permissions)
	}
}
