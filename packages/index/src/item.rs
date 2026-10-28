use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum Item {
	#[tangram_serialize(id = 25)]
	DeleteDelegations(tg::authorization::Subject),

	#[tangram_serialize(id = 22)]
	DeleteCheckout(tg::Id),

	#[tangram_serialize(id = 0)]
	DeletePermission(crate::permission::delete::Arg),

	#[tangram_serialize(id = 1)]
	DeleteGroup(tg::group::Id),

	#[tangram_serialize(id = 2)]
	DeleteGroupMember(crate::group::member::delete::Arg),

	#[tangram_serialize(id = 3)]
	DeleteOrganization(tg::organization::Id),

	#[tangram_serialize(id = 4)]
	DeleteOrganizationMember(crate::organization::member::delete::Arg),

	#[tangram_serialize(id = 5)]
	DeleteSandbox(tg::sandbox::Id),

	#[tangram_serialize(id = 6)]
	DeleteTag(tg::tag::Id),

	#[tangram_serialize(id = 7)]
	DeleteUser(tg::user::Id),

	#[tangram_serialize(id = 30)]
	EnqueuePermissionCapture(crate::permission::capture::enqueue::Arg),

	#[tangram_serialize(id = 8)]
	PutCheckout(crate::checkout::put::Arg),

	#[tangram_serialize(id = 24)]
	PutDelegation(crate::delegation::put::Arg),

	#[tangram_serialize(id = 9)]
	PutPermission(crate::permission::put::Arg),

	#[tangram_serialize(id = 10)]
	PutGroup(crate::group::put::Arg),

	#[tangram_serialize(id = 11)]
	PutGroupMember(crate::group::member::put::Arg),

	#[tangram_serialize(id = 12)]
	PutObject(crate::object::put::Arg),

	#[tangram_serialize(id = 13)]
	PutOrganization(crate::organization::put::Arg),

	#[tangram_serialize(id = 14)]
	PutOrganizationMember(crate::organization::member::put::Arg),

	#[tangram_serialize(id = 20)]
	PutAccountObject(crate::usage::storage::put::ObjectArg),

	#[tangram_serialize(id = 21)]
	PutAccountProcess(crate::usage::storage::put::ProcessArg),

	#[tangram_serialize(id = 15)]
	PutProcess(crate::process::put::Arg),

	#[tangram_serialize(id = 17)]
	PutSandbox(crate::sandbox::put::Arg),

	#[tangram_serialize(id = 31)]
	PutSync(crate::sync::put::Arg),

	#[tangram_serialize(id = 18)]
	PutTag(crate::tag::put::Arg),

	#[tangram_serialize(id = 19)]
	PutUser(crate::user::put::Arg),
}

impl Item {
	/// Get the process that grants authority and the other process that receives it, for a delegation or a permission capture from one process to another.
	#[must_use]
	pub fn process_grant(&self) -> Option<(tg::process::Id, tg::process::Id)> {
		match self {
			Self::EnqueuePermissionCapture(arg) => arg.process_grant(),
			Self::PutDelegation(arg) => arg.process_grant(),
			_ => None,
		}
	}
}
