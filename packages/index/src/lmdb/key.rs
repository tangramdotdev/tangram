use {
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	tangram_client::prelude::*,
};

#[derive(Debug)]
pub enum Key {
	Cache(crate::lmdb::cache::Key),
	Clean(crate::lmdb::clean::Key),
	Grant(crate::lmdb::grant::Key),
	Group(crate::lmdb::group::Key),
	LogCompaction(crate::lmdb::log::Key),
	Node(crate::lmdb::node::Key),
	Object(crate::lmdb::object::Key),
	Organization(crate::lmdb::organization::Key),
	Process(crate::lmdb::process::Key),
	Runner(crate::lmdb::runner::Key),
	Sandbox(crate::lmdb::sandbox::Key),
	Tag(crate::lmdb::tag::Key),
	Update(crate::lmdb::update::Key),
	Usage(crate::lmdb::usage::Key),
	User(crate::lmdb::user::Key),
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
pub enum Kind {
	CacheEntry = 0,
	Object = 1,
	Process = 2,
	Tag = 3,
	CacheEntryDependency = 4,
	DependencyCacheEntry = 5,
	ObjectChild = 6,
	ChildObject = 7,
	ObjectCacheEntry = 8,
	CacheEntryObject = 9,
	ProcessChild = 10,
	ChildProcess = 11,
	ProcessObject = 12,
	ObjectProcess = 13,
	TargetTag = 14,
	Clean = 15,
	ParentTag = 18,
	TagParent = 19,
	User = 20,
	Group = 21,
	Organization = 22,
	GroupMember = 23,
	MemberGroup = 24,
	OrganizationMember = 25,
	MemberOrganization = 26,
	ResourceGrant = 27,
	PrincipalGrant = 28,
	Node = 29,
	Visibility = 30,
	GrantExpiresAt = 31,
	Sandbox = 32,
	CommandCacheableProcess = 33,
	RunnerSandbox = 38,
	SandboxRunner = 39,
	SandboxProcess = 40,
	ProcessSandbox = 41,
	CreatorSandbox = 42,
	OwnerSandbox = 43,
	LogCompaction = 44,
	LogCompactionVersion = 45,
	AccountObject = 51,
	ObjectAccount = 52,
	AccountProcess = 53,
	ProcessAccount = 54,
	UsageAggregate = 55,
	UsageDelta = 56,
	UsageCompaction = 57,
	GrantUpdate = 58,
	GrantUpdateVersion = 59,
	NodeUpdate = 60,
	NodeUpdateVersion = 61,
	StorageUpdate = 62,
	StorageUpdateVersion = 63,
}

impl fdbt::TuplePack for Key {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			Key::Usage(crate::lmdb::usage::Key::AccountObject { account, object }) => (
				Kind::AccountObject.to_i32().unwrap(),
				account.id().to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),
			Key::Usage(crate::lmdb::usage::Key::ObjectAccount { account, object }) => (
				Kind::ObjectAccount.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				account.id().to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Usage(crate::lmdb::usage::Key::AccountProcess { account, process }) => (
				Kind::AccountProcess.to_i32().unwrap(),
				account.id().to_bytes().as_ref(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),
			Key::Usage(crate::lmdb::usage::Key::ProcessAccount { account, process }) => (
				Kind::ProcessAccount.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				account.id().to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Usage(crate::lmdb::usage::Key::Aggregate {
				account,
				partition,
				period,
			}) => (
				Kind::UsageAggregate.to_i32().unwrap(),
				partition,
				i32::from(period.kind() as u8),
				period.start().as_second(),
				account.id().to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),
			Key::Usage(crate::lmdb::usage::Key::Compaction {
				account,
				hour,
				partition,
			}) => (
				Kind::UsageCompaction.to_i32().unwrap(),
				partition,
				hour,
				account.id().to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),
			Key::Usage(crate::lmdb::usage::Key::Delta {
				account,
				hour,
				kind,
				partition,
			}) => (
				Kind::UsageDelta.to_i32().unwrap(),
				partition,
				hour,
				account.id().to_bytes().as_ref(),
				kind.to_i32().unwrap(),
			)
				.pack(w, tuple_depth),

			Key::Cache(crate::lmdb::cache::Key::CacheEntry(id)) => {
				(Kind::CacheEntry.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Object(crate::lmdb::object::Key::Object(id)) => {
				(Kind::Object.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Process(crate::lmdb::process::Key::Process(id)) => {
				(Kind::Process.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Sandbox(crate::lmdb::sandbox::Key::Sandbox(id)) => {
				(Kind::Sandbox.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Runner(crate::lmdb::runner::Key::RunnerSandbox { runner, sandbox }) => (
				Kind::RunnerSandbox.to_i32().unwrap(),
				runner.to_bytes().as_ref(),
				sandbox.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Sandbox(crate::lmdb::sandbox::Key::SandboxRunner { sandbox, runner }) => (
				Kind::SandboxRunner.to_i32().unwrap(),
				sandbox.to_bytes().as_ref(),
				runner.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Sandbox(crate::lmdb::sandbox::Key::SandboxProcess { sandbox, process }) => (
				Kind::SandboxProcess.to_i32().unwrap(),
				sandbox.to_bytes().as_ref(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::lmdb::process::Key::ProcessSandbox { process, sandbox }) => (
				Kind::ProcessSandbox.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				sandbox.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Sandbox(crate::lmdb::sandbox::Key::CreatorSandbox { creator, sandbox }) => (
				Kind::CreatorSandbox.to_i32().unwrap(),
				creator.to_string(),
				sandbox.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Sandbox(crate::lmdb::sandbox::Key::OwnerSandbox { owner, sandbox }) => (
				Kind::OwnerSandbox.to_i32().unwrap(),
				owner.to_string(),
				sandbox.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::lmdb::tag::Key::Tag(id)) => {
				(Kind::Tag.to_i32().unwrap(), id.to_string()).pack(w, tuple_depth)
			},

			Key::Cache(crate::lmdb::cache::Key::CacheEntryDependency {
				cache_entry,
				dependency,
			}) => (
				Kind::CacheEntryDependency.to_i32().unwrap(),
				cache_entry.to_bytes().as_ref(),
				dependency.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Cache(crate::lmdb::cache::Key::DependencyCacheEntry {
				dependency,
				cache_entry,
			}) => (
				Kind::DependencyCacheEntry.to_i32().unwrap(),
				dependency.to_bytes().as_ref(),
				cache_entry.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::lmdb::object::Key::ObjectChild { object, child }) => (
				Kind::ObjectChild.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::lmdb::object::Key::ChildObject { child, object }) => (
				Kind::ChildObject.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::lmdb::object::Key::ObjectCacheEntry {
				object,
				cache_entry,
			}) => (
				Kind::ObjectCacheEntry.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				cache_entry.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::lmdb::object::Key::CacheEntryObject {
				cache_entry,
				object,
			}) => (
				Kind::CacheEntryObject.to_i32().unwrap(),
				cache_entry.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::lmdb::process::Key::ProcessChild { process, child }) => (
				Kind::ProcessChild.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::lmdb::process::Key::ChildProcess { child, parent }) => (
				Kind::ChildProcess.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::lmdb::process::Key::ProcessObject {
				process,
				kind,
				object,
			}) => (
				Kind::ProcessObject.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::lmdb::process::Key::CommandCacheableProcess {
				command,
				process,
			}) => (
				Kind::CommandCacheableProcess.to_i32().unwrap(),
				command.to_bytes().as_ref(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::lmdb::object::Key::ObjectProcess {
				object,
				kind,
				process,
			}) => (
				Kind::ObjectProcess.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::lmdb::tag::Key::TargetTag { target, tag }) => (
				Kind::TargetTag.to_i32().unwrap(),
				target.as_slice(),
				tag.to_string(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::lmdb::tag::Key::ParentTag { parent, name, tag }) => (
				Kind::ParentTag.to_i32().unwrap(),
				parent.as_ref().map(ToString::to_string),
				name,
				tag.to_string(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::lmdb::tag::Key::TagParent { tag, parent, name }) => (
				Kind::TagParent.to_i32().unwrap(),
				tag.to_string(),
				parent.as_ref().map(ToString::to_string),
				name,
			)
				.pack(w, tuple_depth),

			Key::User(crate::lmdb::user::Key::User(user)) => (
				Kind::User.to_i32().unwrap(),
				tg::Id::from(user.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Group(crate::lmdb::group::Key::Group(group)) => (
				Kind::Group.to_i32().unwrap(),
				tg::Id::from(group.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Organization(crate::lmdb::organization::Key::Organization(organization)) => (
				Kind::Organization.to_i32().unwrap(),
				tg::Id::from(organization.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Group(crate::lmdb::group::Key::GroupMember { group, member }) => (
				Kind::GroupMember.to_i32().unwrap(),
				tg::Id::from(group.clone()).to_bytes().as_ref(),
				tg::Id::from(member.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Group(crate::lmdb::group::Key::MemberGroup { member, group }) => (
				Kind::MemberGroup.to_i32().unwrap(),
				tg::Id::from(member.clone()).to_bytes().as_ref(),
				tg::Id::from(group.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Organization(crate::lmdb::organization::Key::OrganizationMember {
				organization,
				member,
			}) => (
				Kind::OrganizationMember.to_i32().unwrap(),
				tg::Id::from(organization.clone()).to_bytes().as_ref(),
				tg::Id::from(member.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Organization(crate::lmdb::organization::Key::MemberOrganization {
				member,
				organization,
			}) => (
				Kind::MemberOrganization.to_i32().unwrap(),
				tg::Id::from(member.clone()).to_bytes().as_ref(),
				tg::Id::from(organization.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Grant(crate::lmdb::grant::Key::ResourceGrant {
				resource,
				principal,
				creator,
				permission,
			}) => (
				Kind::ResourceGrant.to_i32().unwrap(),
				resource.to_bytes().as_ref(),
				principal.to_string(),
				permission.to_string(),
				creator.as_ref().map(ToString::to_string),
			)
				.pack(w, tuple_depth),

			Key::Grant(crate::lmdb::grant::Key::PrincipalGrant {
				principal,
				resource,
				creator,
				permission,
			}) => (
				Kind::PrincipalGrant.to_i32().unwrap(),
				principal.to_string(),
				resource.to_bytes().as_ref(),
				permission.to_string(),
				creator.as_ref().map(ToString::to_string),
			)
				.pack(w, tuple_depth),

			Key::Node(crate::lmdb::node::Key::Node(specifier)) => {
				(Kind::Node.to_i32().unwrap(), specifier.to_string()).pack(w, tuple_depth)
			},

			Key::Grant(crate::lmdb::grant::Key::Visibility {
				resource,
				principal,
				grant_resource,
				creator,
				permission,
			}) => (
				Kind::Visibility.to_i32().unwrap(),
				resource.to_bytes().as_ref(),
				principal.to_string(),
				grant_resource.to_bytes().as_ref(),
				permission.to_string(),
				creator.as_ref().map(ToString::to_string),
			)
				.pack(w, tuple_depth),

			Key::Grant(crate::lmdb::grant::Key::GrantExpiresAt {
				expires_at,
				resource,
				principal,
				creator,
				permission,
				source,
			}) => (
				Kind::GrantExpiresAt.to_i32().unwrap(),
				expires_at,
				resource.to_bytes().as_ref(),
				principal.to_string(),
				permission.to_string(),
				creator.as_ref().map(ToString::to_string),
				source.to_i32(),
			)
				.pack(w, tuple_depth),

			Key::Clean(key) => {
				Kind::Clean.to_i32().unwrap().pack(w, tuple_depth)?;
				match key {
					crate::lmdb::clean::Key::AccountObject {
						account,
						object,
						touched_at,
					} => (
						touched_at,
						crate::lmdb::clean::ItemKind::AccountObject
							.to_i32()
							.unwrap(),
						account.id().to_bytes().as_ref(),
						object.to_bytes().as_ref(),
					)
						.pack(w, tuple_depth),
					crate::lmdb::clean::Key::AccountProcess {
						account,
						process,
						touched_at,
					} => (
						touched_at,
						crate::lmdb::clean::ItemKind::AccountProcess
							.to_i32()
							.unwrap(),
						account.id().to_bytes().as_ref(),
						process.to_bytes().as_ref(),
					)
						.pack(w, tuple_depth),
					crate::lmdb::clean::Key::CacheEntry { id, touched_at } => (
						touched_at,
						crate::lmdb::clean::ItemKind::CacheEntry.to_i32().unwrap(),
						id.to_bytes().as_ref(),
					)
						.pack(w, tuple_depth),
					crate::lmdb::clean::Key::Object { id, touched_at } => (
						touched_at,
						crate::lmdb::clean::ItemKind::Object.to_i32().unwrap(),
						id.to_bytes().as_ref(),
					)
						.pack(w, tuple_depth),
					crate::lmdb::clean::Key::Process { id, touched_at } => (
						touched_at,
						crate::lmdb::clean::ItemKind::Process.to_i32().unwrap(),
						id.to_bytes().as_ref(),
					)
						.pack(w, tuple_depth),
					crate::lmdb::clean::Key::Sandbox { id, touched_at } => (
						touched_at,
						crate::lmdb::clean::ItemKind::Sandbox.to_i32().unwrap(),
						id.to_bytes().as_ref(),
					)
						.pack(w, tuple_depth),
				}
			},

			Key::LogCompaction(crate::lmdb::log::Key::Identity(process)) => (
				Kind::LogCompaction.to_i32().unwrap(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::LogCompaction(crate::lmdb::log::Key::Version { process, version }) => (
				Kind::LogCompactionVersion.to_i32().unwrap(),
				version,
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Update(crate::lmdb::update::Key::Update { id, kind }) => {
				let key_kind = match kind {
					crate::lmdb::update::Kind::Grant(_) => Kind::GrantUpdate,
					crate::lmdb::update::Kind::Node => Kind::NodeUpdate,
					crate::lmdb::update::Kind::Storage(_) => Kind::StorageUpdate,
				};
				key_kind.to_i32().unwrap().pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				let mut offset = id.as_ref().pack(w, tuple_depth)?;
				offset += pack_update_kind(w, tuple_depth, kind)?;
				Ok(offset)
			},

			Key::Update(crate::lmdb::update::Key::UpdateVersion { id, kind, version }) => {
				let key_kind = match kind {
					crate::lmdb::update::Kind::Grant(_) => Kind::GrantUpdateVersion,
					crate::lmdb::update::Kind::Node => Kind::NodeUpdateVersion,
					crate::lmdb::update::Kind::Storage(_) => Kind::StorageUpdateVersion,
				};
				key_kind.to_i32().unwrap().pack(w, tuple_depth)?;
				let mut offset = version.pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				offset += id.as_ref().pack(w, tuple_depth)?;
				offset += pack_update_kind(w, tuple_depth, kind)?;
				Ok(offset)
			},
		}
	}
}

impl fdbt::TupleUnpack<'_> for Key {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, kind_value) = i32::unpack(input, tuple_depth)?;
		let kind =
			Kind::from_i32(kind_value).ok_or(fdbt::PackError::Message("invalid kind".into()))?;

		match kind {
			Kind::AccountObject => {
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, object): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = crate::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let object = tg::object::Id::from_slice(&object)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::Usage(crate::lmdb::usage::Key::AccountObject { account, object });
				Ok((input, key))
			},
			Kind::ObjectAccount => {
				let (input, object): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = crate::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let key = Key::Usage(crate::lmdb::usage::Key::ObjectAccount { account, object });
				Ok((input, key))
			},

			Kind::AccountProcess => {
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, process): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = crate::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let process = tg::process::Id::from_slice(&process)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::Usage(crate::lmdb::usage::Key::AccountProcess { account, process });
				Ok((input, key))
			},
			Kind::ProcessAccount => {
				let (input, process): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = crate::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let key = Key::Usage(crate::lmdb::usage::Key::ProcessAccount { account, process });
				Ok((input, key))
			},

			Kind::UsageAggregate => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, period_kind): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, start): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = crate::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let kind = match period_kind {
					0 => crate::usage::PeriodKind::Day,
					1 => crate::usage::PeriodKind::Hour,
					2 => crate::usage::PeriodKind::Month,
					3 => crate::usage::PeriodKind::Week,
					_ => return Err(fdbt::PackError::Message("invalid usage period kind".into())),
				};
				let period = crate::usage::Period::from_kind_and_start(kind, start)
					.map_err(|_| fdbt::PackError::Message("invalid usage period".into()))?;
				let key = Key::Usage(crate::lmdb::usage::Key::Aggregate {
					account,
					partition,
					period,
				});
				Ok((input, key))
			},
			Kind::UsageCompaction => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, hour): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = crate::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let key = Key::Usage(crate::lmdb::usage::Key::Compaction {
					account,
					hour,
					partition,
				});
				Ok((input, key))
			},
			Kind::UsageDelta => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, hour): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, delta_kind): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = crate::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let kind = crate::usage::DeltaKind::from_i32(delta_kind)
					.ok_or_else(|| fdbt::PackError::Message("invalid usage delta kind".into()))?;
				let key = Key::Usage(crate::lmdb::usage::Key::Delta {
					account,
					hour,
					kind,
					partition,
				});
				Ok((input, key))
			},

			Kind::CacheEntry => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::artifact::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				Ok((input, Key::Cache(crate::lmdb::cache::Key::CacheEntry(id))))
			},

			Kind::Object => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::object::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((input, Key::Object(crate::lmdb::object::Key::Object(id))))
			},

			Kind::Process => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::process::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((input, Key::Process(crate::lmdb::process::Key::Process(id))))
			},

			Kind::Sandbox => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::sandbox::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
				Ok((input, Key::Sandbox(crate::lmdb::sandbox::Key::Sandbox(id))))
			},

			Kind::RunnerSandbox => {
				let (input, runner): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, sandbox): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let runner = tg::runner::Id::from_slice(&runner)
					.map_err(|_| fdbt::PackError::Message("invalid runner id".into()))?;
				let sandbox = tg::sandbox::Id::from_slice(&sandbox)
					.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
				Ok((
					input,
					Key::Runner(crate::lmdb::runner::Key::RunnerSandbox { runner, sandbox }),
				))
			},

			Kind::SandboxRunner => {
				let (input, sandbox): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, runner): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let sandbox = tg::sandbox::Id::from_slice(&sandbox)
					.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
				let runner = tg::runner::Id::from_slice(&runner)
					.map_err(|_| fdbt::PackError::Message("invalid runner id".into()))?;
				Ok((
					input,
					Key::Sandbox(crate::lmdb::sandbox::Key::SandboxRunner { sandbox, runner }),
				))
			},

			Kind::SandboxProcess => {
				let (input, sandbox): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, process): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let sandbox = tg::sandbox::Id::from_slice(&sandbox)
					.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
				let process = tg::process::Id::from_slice(&process)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((
					input,
					Key::Sandbox(crate::lmdb::sandbox::Key::SandboxProcess { sandbox, process }),
				))
			},

			Kind::ProcessSandbox => {
				let (input, process): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, sandbox): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let sandbox = tg::sandbox::Id::from_slice(&sandbox)
					.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
				Ok((
					input,
					Key::Process(crate::lmdb::process::Key::ProcessSandbox { process, sandbox }),
				))
			},

			Kind::CreatorSandbox => {
				let (input, creator): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, sandbox): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let creator = creator
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid sandbox creator".into()))?;
				let sandbox = tg::sandbox::Id::from_slice(&sandbox)
					.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
				Ok((
					input,
					Key::Sandbox(crate::lmdb::sandbox::Key::CreatorSandbox { creator, sandbox }),
				))
			},

			Kind::OwnerSandbox => {
				let (input, owner): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, sandbox): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let owner = owner
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid sandbox owner".into()))?;
				let sandbox = tg::sandbox::Id::from_slice(&sandbox)
					.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
				Ok((
					input,
					Key::Sandbox(crate::lmdb::sandbox::Key::OwnerSandbox { owner, sandbox }),
				))
			},

			Kind::Tag => {
				let (input, id): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = id
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid tag id".into()))?;
				Ok((input, Key::Tag(crate::lmdb::tag::Key::Tag(id))))
			},

			Kind::CacheEntryDependency => {
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, dependency_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let dependency = tg::artifact::Id::from_slice(&dependency_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::Cache(crate::lmdb::cache::Key::CacheEntryDependency {
					cache_entry,
					dependency,
				});
				Ok((input, key))
			},

			Kind::DependencyCacheEntry => {
				let (input, dependency_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let dependency = tg::artifact::Id::from_slice(&dependency_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::Cache(crate::lmdb::cache::Key::DependencyCacheEntry {
					dependency,
					cache_entry,
				});
				Ok((input, key))
			},

			Kind::ObjectChild => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let child = tg::object::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((
					input,
					Key::Object(crate::lmdb::object::Key::ObjectChild { object, child }),
				))
			},

			Kind::ChildObject => {
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let child = tg::object::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((
					input,
					Key::Object(crate::lmdb::object::Key::ChildObject { child, object }),
				))
			},

			Kind::ObjectCacheEntry => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::Object(crate::lmdb::object::Key::ObjectCacheEntry {
					object,
					cache_entry,
				});
				Ok((input, key))
			},

			Kind::CacheEntryObject => {
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::Object(crate::lmdb::object::Key::CacheEntryObject {
					cache_entry,
					object,
				});
				Ok((input, key))
			},

			Kind::ProcessChild => {
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let child = tg::process::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((
					input,
					Key::Process(crate::lmdb::process::Key::ProcessChild { process, child }),
				))
			},

			Kind::ChildProcess => {
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, parent_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let child = tg::process::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let parent = tg::process::Id::from_slice(&parent_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((
					input,
					Key::Process(crate::lmdb::process::Key::ChildProcess { child, parent }),
				))
			},

			Kind::ProcessObject => {
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind_value) = i32::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let kind = crate::process::object::Kind::from_i32(kind_value).ok_or(
					fdbt::PackError::Message("invalid process object kind".into()),
				)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::Process(crate::lmdb::process::Key::ProcessObject {
					process,
					kind,
					object,
				});
				Ok((input, key))
			},

			Kind::ObjectProcess => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind_value) = i32::unpack(input, tuple_depth)?;
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let kind = crate::process::object::Kind::from_i32(kind_value).ok_or(
					fdbt::PackError::Message("invalid process object kind".into()),
				)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::Object(crate::lmdb::object::Key::ObjectProcess {
					object,
					kind,
					process,
				});
				Ok((input, key))
			},

			Kind::CommandCacheableProcess => {
				let (input, command_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let command = tg::object::Id::from_slice(&command_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid command id".into()))?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::Process(crate::lmdb::process::Key::CommandCacheableProcess {
					command,
					process,
				});
				Ok((input, key))
			},

			Kind::TargetTag => {
				let (input, target): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let tag = tag
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid tag id".into()))?;
				Ok((
					input,
					Key::Tag(crate::lmdb::tag::Key::TargetTag { target, tag }),
				))
			},

			Kind::ParentTag => {
				let (input, parent): (_, Option<String>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, name): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let parent = parent
					.map(|parent| {
						parent
							.parse()
							.map_err(|_| fdbt::PackError::Message("invalid parent id".into()))
					})
					.transpose()?;
				let tag = tag
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid tag id".into()))?;
				Ok((
					input,
					Key::Tag(crate::lmdb::tag::Key::ParentTag { parent, name, tag }),
				))
			},

			Kind::TagParent => {
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, parent): (_, Option<String>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, name): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let tag = tag
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid tag id".into()))?;
				let parent = parent
					.map(|parent| {
						parent
							.parse()
							.map_err(|_| fdbt::PackError::Message("invalid parent id".into()))
					})
					.transpose()?;
				Ok((
					input,
					Key::Tag(crate::lmdb::tag::Key::TagParent { tag, parent, name }),
				))
			},

			Kind::User => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid user id".into()))?;
				let id = tg::user::Id::try_from(id)
					.map_err(|_| fdbt::PackError::Message("invalid user id".into()))?;
				Ok((input, Key::User(crate::lmdb::user::Key::User(id))))
			},

			Kind::Group => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid group id".into()))?;
				let id = tg::group::Id::try_from(id)
					.map_err(|_| fdbt::PackError::Message("invalid group id".into()))?;
				Ok((input, Key::Group(crate::lmdb::group::Key::Group(id))))
			},

			Kind::Organization => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid organization id".into()))?;
				let id = tg::organization::Id::try_from(id)
					.map_err(|_| fdbt::PackError::Message("invalid organization id".into()))?;
				let key = Key::Organization(crate::lmdb::organization::Key::Organization(id));
				Ok((input, key))
			},

			Kind::GroupMember => {
				let (input, group_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, member_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let group = tg::Id::from_slice(&group_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid group id".into()))?;
				let group = tg::group::Id::try_from(group)
					.map_err(|_| fdbt::PackError::Message("invalid group id".into()))?;
				let member = tg::Id::from_slice(&member_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid group member".into()))?;
				let member = tg::group::Member::try_from(member)
					.map_err(|_| fdbt::PackError::Message("invalid group member".into()))?;
				let key = Key::Group(crate::lmdb::group::Key::GroupMember { group, member });
				Ok((input, key))
			},

			Kind::MemberGroup => {
				let (input, member_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, group_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let member = tg::Id::from_slice(&member_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid group member".into()))?;
				let member = tg::group::Member::try_from(member)
					.map_err(|_| fdbt::PackError::Message("invalid group member".into()))?;
				let group = tg::Id::from_slice(&group_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid group id".into()))?;
				let group = tg::group::Id::try_from(group)
					.map_err(|_| fdbt::PackError::Message("invalid group id".into()))?;
				let key = Key::Group(crate::lmdb::group::Key::MemberGroup { member, group });
				Ok((input, key))
			},

			Kind::OrganizationMember => {
				let (input, organization_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, member_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let organization = tg::Id::from_slice(&organization_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid organization id".into()))?;
				let organization = tg::organization::Id::try_from(organization)
					.map_err(|_| fdbt::PackError::Message("invalid organization id".into()))?;
				let member = tg::Id::from_slice(&member_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid organization member".into()))?;
				let member = tg::organization::Member::try_from(member)
					.map_err(|_| fdbt::PackError::Message("invalid organization member".into()))?;
				let key = Key::Organization(crate::lmdb::organization::Key::OrganizationMember {
					organization,
					member,
				});
				Ok((input, key))
			},

			Kind::MemberOrganization => {
				let (input, member_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, organization_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let member = tg::Id::from_slice(&member_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid organization member".into()))?;
				let member = tg::organization::Member::try_from(member)
					.map_err(|_| fdbt::PackError::Message("invalid organization member".into()))?;
				let organization = tg::Id::from_slice(&organization_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid organization id".into()))?;
				let organization = tg::organization::Id::try_from(organization)
					.map_err(|_| fdbt::PackError::Message("invalid organization id".into()))?;
				let key = Key::Organization(crate::lmdb::organization::Key::MemberOrganization {
					member,
					organization,
				});
				Ok((input, key))
			},

			Kind::ResourceGrant => {
				let (input, resource_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, principal): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, permission): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, creator): (_, Option<String>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let resource = tg::Id::from_slice(&resource_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid resource id".into()))?;
				let principal = principal
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid grant principal".into()))?;
				let creator = creator
					.map(|creator| {
						creator
							.parse()
							.map_err(|_| fdbt::PackError::Message("invalid grant creator".into()))
					})
					.transpose()?;
				let permission = permission
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid grant permission".into()))?;
				let key = Key::Grant(crate::lmdb::grant::Key::ResourceGrant {
					resource,
					principal,
					creator,
					permission,
				});
				Ok((input, key))
			},

			Kind::PrincipalGrant => {
				let (input, principal): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, resource_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, permission): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, creator): (_, Option<String>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let principal = principal
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid grant principal".into()))?;
				let resource = tg::Id::from_slice(&resource_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid resource id".into()))?;
				let creator = creator
					.map(|creator| {
						creator
							.parse()
							.map_err(|_| fdbt::PackError::Message("invalid grant creator".into()))
					})
					.transpose()?;
				let permission = permission
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid grant permission".into()))?;
				let key = Key::Grant(crate::lmdb::grant::Key::PrincipalGrant {
					principal,
					resource,
					creator,
					permission,
				});
				Ok((input, key))
			},

			Kind::Node => {
				let (input, specifier): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let specifier = specifier
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid specifier".into()))?;
				Ok((input, Key::Node(crate::lmdb::node::Key::Node(specifier))))
			},

			Kind::Visibility => {
				let (input, resource_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, principal): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, grant_resource_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, permission): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, creator): (_, Option<String>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let resource = tg::Id::from_slice(&resource_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid resource id".into()))?;
				let principal = principal
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid grant principal".into()))?;
				let grant_resource = tg::Id::from_slice(&grant_resource_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid resource id".into()))?;
				let creator = creator
					.map(|creator| {
						creator
							.parse()
							.map_err(|_| fdbt::PackError::Message("invalid grant creator".into()))
					})
					.transpose()?;
				let permission = permission
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid grant permission".into()))?;
				let key = Key::Grant(crate::lmdb::grant::Key::Visibility {
					resource,
					principal,
					grant_resource,
					creator,
					permission,
				});
				Ok((input, key))
			},

			Kind::GrantExpiresAt => {
				let (input, expires_at): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, resource_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, principal): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, permission): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, creator): (_, Option<String>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, source): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let resource = tg::Id::from_slice(&resource_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid resource id".into()))?;
				let principal = principal
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid grant principal".into()))?;
				let creator = creator
					.map(|creator| {
						creator
							.parse()
							.map_err(|_| fdbt::PackError::Message("invalid grant creator".into()))
					})
					.transpose()?;
				let permission = permission
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid grant permission".into()))?;
				let source = crate::lmdb::grant::GrantSource::from_i32(source)
					.ok_or_else(|| fdbt::PackError::Message("invalid grant source".into()))?;
				let key = Key::Grant(crate::lmdb::grant::Key::GrantExpiresAt {
					expires_at,
					resource,
					principal,
					creator,
					permission,
					source,
				});
				Ok((input, key))
			},

			Kind::Clean => {
				let (input, touched_at): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind_value): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let kind = crate::lmdb::clean::ItemKind::from_i32(kind_value)
					.ok_or(fdbt::PackError::Message("invalid cleanup item kind".into()))?;
				let (input, key) = match kind {
					crate::lmdb::clean::ItemKind::AccountObject => {
						let (input, account): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let (input, object): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let account = tg::Id::from_slice(&account).map_err(|_| {
							fdbt::PackError::Message("invalid usage account".into())
						})?;
						let account = crate::usage::Account::try_from(account).map_err(|_| {
							fdbt::PackError::Message("invalid usage account".into())
						})?;
						let object = tg::object::Id::from_slice(&object)
							.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
						let key = crate::lmdb::clean::Key::AccountObject {
							account,
							object,
							touched_at,
						};
						(input, key)
					},
					crate::lmdb::clean::ItemKind::AccountProcess => {
						let (input, account): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let (input, process): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let account = tg::Id::from_slice(&account).map_err(|_| {
							fdbt::PackError::Message("invalid usage account".into())
						})?;
						let account = crate::usage::Account::try_from(account).map_err(|_| {
							fdbt::PackError::Message("invalid usage account".into())
						})?;
						let process = tg::process::Id::from_slice(&process)
							.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
						let key = crate::lmdb::clean::Key::AccountProcess {
							account,
							process,
							touched_at,
						};
						(input, key)
					},
					crate::lmdb::clean::ItemKind::CacheEntry => {
						let (input, id): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let id = tg::object::Id::from_slice(&id)
							.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
						let id = tg::artifact::Id::try_from(id)
							.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
						let key = crate::lmdb::clean::Key::CacheEntry { id, touched_at };
						(input, key)
					},
					crate::lmdb::clean::ItemKind::Object => {
						let (input, id): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let id = tg::object::Id::from_slice(&id)
							.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
						let key = crate::lmdb::clean::Key::Object { id, touched_at };
						(input, key)
					},
					crate::lmdb::clean::ItemKind::Process => {
						let (input, id): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let id = tg::process::Id::from_slice(&id)
							.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
						let key = crate::lmdb::clean::Key::Process { id, touched_at };
						(input, key)
					},
					crate::lmdb::clean::ItemKind::Sandbox => {
						let (input, id): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let id = tg::sandbox::Id::from_slice(&id)
							.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
						let key = crate::lmdb::clean::Key::Sandbox { id, touched_at };
						(input, key)
					},
				};
				let key = Key::Clean(key);
				Ok((input, key))
			},

			Kind::LogCompaction => {
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((
					input,
					Key::LogCompaction(crate::lmdb::log::Key::Identity(process)),
				))
			},

			Kind::LogCompactionVersion => {
				let (input, version): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::LogCompaction(crate::lmdb::log::Key::Version { process, version });
				Ok((input, key))
			},

			Kind::GrantUpdate | Kind::NodeUpdate | Kind::StorageUpdate => {
				let update_kind = match kind {
					Kind::GrantUpdate => crate::update::Kind::Grant,
					Kind::NodeUpdate => crate::update::Kind::Node,
					Kind::StorageUpdate => crate::update::Kind::Storage,
					_ => unreachable!(),
				};
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = if let Ok(id) = tg::process::Id::try_from(id.clone()) {
					tg::Either::Right(id)
				} else if let Ok(id) = tg::object::Id::try_from(id) {
					tg::Either::Left(id)
				} else {
					return Err(fdbt::PackError::Message("invalid id".into()));
				};
				let (input, kind) = unpack_update_kind(input, tuple_depth, update_kind)?;
				Ok((
					input,
					Key::Update(crate::lmdb::update::Key::Update { id, kind }),
				))
			},

			Kind::GrantUpdateVersion | Kind::NodeUpdateVersion | Kind::StorageUpdateVersion => {
				let update_kind = match kind {
					Kind::GrantUpdateVersion => crate::update::Kind::Grant,
					Kind::NodeUpdateVersion => crate::update::Kind::Node,
					Kind::StorageUpdateVersion => crate::update::Kind::Storage,
					_ => unreachable!(),
				};
				let (input, version): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = if let Ok(id) = tg::process::Id::try_from(id.clone()) {
					tg::Either::Right(id)
				} else if let Ok(id) = tg::object::Id::try_from(id) {
					tg::Either::Left(id)
				} else {
					return Err(fdbt::PackError::Message("invalid id".into()));
				};
				let (input, kind) = unpack_update_kind(input, tuple_depth, update_kind)?;
				Ok((
					input,
					Key::Update(crate::lmdb::update::Key::UpdateVersion { id, kind, version }),
				))
			},
		}
	}
}

fn pack_update_kind<W: std::io::Write>(
	w: &mut W,
	tuple_depth: fdbt::TupleDepth,
	kind: &crate::lmdb::update::Kind,
) -> std::io::Result<fdbt::VersionstampOffset> {
	match kind {
		crate::lmdb::update::Kind::Grant(principal) => principal.to_string().pack(w, tuple_depth),
		crate::lmdb::update::Kind::Node => ().pack(w, tuple_depth),
		crate::lmdb::update::Kind::Storage(kind) => match kind {
			crate::lmdb::update::StorageKind::Add {
				account,
				touched_at,
			} => {
				let mut offset = 0i32.pack(w, tuple_depth)?;
				offset += account.id().to_bytes().as_ref().pack(w, tuple_depth)?;
				offset += touched_at.pack(w, tuple_depth)?;
				Ok(offset)
			},
			crate::lmdb::update::StorageKind::Clean(account) => {
				let mut offset = 1i32.pack(w, tuple_depth)?;
				offset += account.id().to_bytes().as_ref().pack(w, tuple_depth)?;
				Ok(offset)
			},
			crate::lmdb::update::StorageKind::CleanAll => 2i32.pack(w, tuple_depth),
			crate::lmdb::update::StorageKind::Propagate {
				account,
				touched_at,
			} => {
				let mut offset = 3i32.pack(w, tuple_depth)?;
				offset += account.id().to_bytes().as_ref().pack(w, tuple_depth)?;
				offset += touched_at.pack(w, tuple_depth)?;
				Ok(offset)
			},
		},
	}
}

fn unpack_update_kind(
	input: &[u8],
	tuple_depth: fdbt::TupleDepth,
	kind: crate::update::Kind,
) -> Result<(&[u8], crate::lmdb::update::Kind), fdbt::PackError> {
	match kind {
		crate::update::Kind::Grant => {
			let (input, principal): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
			let principal = principal
				.parse()
				.map_err(|_| fdbt::PackError::Message("invalid grant principal".into()))?;
			Ok((input, crate::lmdb::update::Kind::Grant(principal)))
		},
		crate::update::Kind::Node => Ok((input, crate::lmdb::update::Kind::Node)),
		crate::update::Kind::Storage => {
			let (input, kind): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
			let (input, kind) = match kind {
				0 | 3 => {
					let (input, account): (_, Vec<u8>) =
						fdbt::TupleUnpack::unpack(input, tuple_depth)?;
					let (input, touched_at): (_, i64) =
						fdbt::TupleUnpack::unpack(input, tuple_depth)?;
					let account = tg::Id::from_slice(&account)
						.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
					let account = crate::usage::Account::try_from(account)
						.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
					let kind = match kind {
						0 => crate::lmdb::update::StorageKind::Add {
							account,
							touched_at,
						},
						3 => crate::lmdb::update::StorageKind::Propagate {
							account,
							touched_at,
						},
						_ => unreachable!(),
					};
					(input, kind)
				},
				1 => {
					let (input, account): (_, Vec<u8>) =
						fdbt::TupleUnpack::unpack(input, tuple_depth)?;
					let account = tg::Id::from_slice(&account)
						.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
					let account = crate::usage::Account::try_from(account)
						.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
					(input, crate::lmdb::update::StorageKind::Clean(account))
				},
				2 => (input, crate::lmdb::update::StorageKind::CleanAll),
				_ => {
					return Err(fdbt::PackError::Message(
						"invalid storage update kind".into(),
					));
				},
			};
			Ok((input, crate::lmdb::update::Kind::Storage(kind)))
		},
	}
}
