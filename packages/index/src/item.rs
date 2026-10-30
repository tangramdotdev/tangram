use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum Item {
	#[tangram_serialize(id = 25)]
	DeleteDelegations(Box<crate::delegation::delete::Arg>),

	#[tangram_serialize(id = 22)]
	DeleteCheckout(Box<crate::checkout::delete::Arg>),

	#[tangram_serialize(id = 0)]
	DeletePermission(Box<crate::permission::delete::Arg>),

	#[tangram_serialize(id = 1)]
	DeleteGroup(Box<crate::group::delete::Arg>),

	#[tangram_serialize(id = 2)]
	DeleteGroupMember(Box<crate::group::member::delete::Arg>),

	#[tangram_serialize(id = 3)]
	DeleteOrganization(Box<crate::organization::delete::Arg>),

	#[tangram_serialize(id = 4)]
	DeleteOrganizationMember(Box<crate::organization::member::delete::Arg>),

	#[tangram_serialize(id = 5)]
	DeleteSandbox(Box<crate::sandbox::delete::Arg>),

	#[tangram_serialize(id = 6)]
	DeleteTag(Box<crate::tag::delete::Arg>),

	#[tangram_serialize(id = 7)]
	DeleteUser(Box<crate::user::delete::Arg>),

	#[tangram_serialize(id = 30)]
	EnqueuePermissionCapture(Box<crate::permission::capture::enqueue::Arg>),

	#[tangram_serialize(id = 8)]
	PutCheckout(Box<crate::checkout::put::Arg>),

	#[tangram_serialize(id = 24)]
	PutDelegation(Box<crate::delegation::put::Arg>),

	#[tangram_serialize(id = 9)]
	PutPermission(Box<crate::permission::put::Arg>),

	#[tangram_serialize(id = 10)]
	PutGroup(Box<crate::group::put::Arg>),

	#[tangram_serialize(id = 11)]
	PutGroupMember(Box<crate::group::member::put::Arg>),

	#[tangram_serialize(id = 12)]
	PutObject(Box<crate::object::put::Arg>),

	#[tangram_serialize(id = 13)]
	PutOrganization(Box<crate::organization::put::Arg>),

	#[tangram_serialize(id = 14)]
	PutOrganizationMember(Box<crate::organization::member::put::Arg>),

	#[tangram_serialize(id = 20)]
	PutAccountObject(Box<crate::usage::storage::put::ObjectArg>),

	#[tangram_serialize(id = 21)]
	PutAccountProcess(Box<crate::usage::storage::put::ProcessArg>),

	#[tangram_serialize(id = 15)]
	PutProcess(Box<crate::process::put::Arg>),

	#[tangram_serialize(id = 17)]
	PutSandbox(Box<crate::sandbox::put::Arg>),

	#[tangram_serialize(id = 31)]
	PutSync(Box<crate::sync::put::Arg>),

	#[tangram_serialize(id = 18)]
	PutTag(Box<crate::tag::put::Arg>),

	#[tangram_serialize(id = 19)]
	PutUser(Box<crate::user::put::Arg>),
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
