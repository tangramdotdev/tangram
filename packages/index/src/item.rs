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

	#[tangram_serialize(id = 33)]
	DeleteSchedulerSandbox(Box<crate::scheduler::sandbox::delete::Arg>),

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

	#[tangram_serialize(id = 32)]
	PutRunner(Box<crate::runner::put::Arg>),

	#[tangram_serialize(id = 17)]
	PutSandbox(Box<crate::sandbox::put::Arg>),

	#[tangram_serialize(id = 34)]
	PutSchedulerSandbox(Box<crate::scheduler::sandbox::put::Arg>),

	#[tangram_serialize(id = 31)]
	PutSync(Box<crate::sync::put::Arg>),

	#[tangram_serialize(id = 18)]
	PutTag(Box<crate::tag::put::Arg>),

	#[tangram_serialize(id = 19)]
	PutUser(Box<crate::user::put::Arg>),
}

/// What an item changes in common with other items, so that items with the same affinity run in sequence in one transaction rather than conflict in concurrent ones.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Affinity {
	Sandbox(tg::sandbox::Id),
}

impl Item {
	/// Get the affinity of this item, which is the sandbox that a sandbox put changes, or the sandbox of the process that a process put changes if the put knows it.
	#[must_use]
	pub fn affinity(&self) -> Option<Affinity> {
		match self {
			Self::PutProcess(arg) => arg
				.sandbox
				.clone()
				.or_else(|| arg.data.as_ref().and_then(|data| data.sandbox.clone()))
				.map(Affinity::Sandbox),
			Self::PutSandbox(arg) => Some(Affinity::Sandbox(arg.id.clone())),
			_ => None,
		}
	}
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

impl Item {
	/// Get the process that this item puts, either directly or carried by a sandbox.
	#[must_use]
	pub fn process(&self) -> Option<&crate::process::put::Arg> {
		match self {
			Self::PutProcess(arg) => Some(arg),
			Self::PutSandbox(arg) => arg.process.as_deref(),
			_ => None,
		}
	}
}

impl Affinity {
	/// Hash the affinity with 64-bit FNV-1a over its sandbox ID's string, which is the same on every server and in every process and version.
	#[must_use]
	pub fn stable_hash(&self) -> u64 {
		let Self::Sandbox(id) = self;
		let mut hash = 0xcbf2_9ce4_8422_2325_u64;
		for byte in id.to_string().bytes() {
			hash ^= u64::from(byte);
			hash = hash.wrapping_mul(0x0100_0000_01b3);
		}
		hash
	}
}
