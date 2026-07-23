use {
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub enum Key {
	Cache(crate::fdb::cache::Key),
	Clean(crate::fdb::clean::Key),
	Finalization(crate::fdb::finalization::Key),
	Grant(crate::fdb::grant::Key),
	Group(crate::fdb::group::Key),
	Node(crate::fdb::node::Key),
	Object(crate::fdb::object::Key),
	Organization(crate::fdb::organization::Key),
	Process(crate::fdb::process::Key),
	Runner(crate::fdb::runner::Key),
	Sandbox(crate::fdb::sandbox::Key),
	Tag(crate::fdb::tag::Key),
	Update(crate::fdb::update::Key),
	User(crate::fdb::user::Key),
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
	ItemTag = 14,
	Clean = 15,
	Update = 16,
	UpdateVersion = 17,
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
	Runner = 35,
	SchedulerRunner = 36,
	RunnerScheduler = 37,
	RunnerSandbox = 38,
	SandboxRunner = 39,
	SandboxProcess = 40,
	ProcessSandbox = 41,
	CreatorSandbox = 42,
	OwnerSandbox = 43,
	ProcessFinalization = 45,
	ProcessFinalizationVersion = 47,
	SandboxFinalization = 48,
	SandboxFinalizationVersion = 50,
}

impl fdbt::TuplePack for Key {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			Key::Cache(crate::fdb::cache::Key::CacheEntry(id)) => {
				(Kind::CacheEntry.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Object(crate::fdb::object::Key::Object(id)) => {
				(Kind::Object.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Process(crate::fdb::process::Key::Process(id)) => {
				(Kind::Process.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Sandbox(crate::fdb::sandbox::Key::Sandbox(id)) => {
				(Kind::Sandbox.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Runner(crate::fdb::runner::Key::Runner(id)) => {
				(Kind::Runner.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Runner(crate::fdb::runner::Key::SchedulerRunner { scheduler, runner }) => (
				Kind::SchedulerRunner.to_i32().unwrap(),
				scheduler.to_bytes().as_ref(),
				runner.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Runner(crate::fdb::runner::Key::RunnerScheduler { runner, scheduler }) => (
				Kind::RunnerScheduler.to_i32().unwrap(),
				runner.to_bytes().as_ref(),
				scheduler.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Runner(crate::fdb::runner::Key::RunnerSandbox { runner, sandbox }) => (
				Kind::RunnerSandbox.to_i32().unwrap(),
				runner.to_bytes().as_ref(),
				sandbox.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Sandbox(crate::fdb::sandbox::Key::SandboxRunner { sandbox, runner }) => (
				Kind::SandboxRunner.to_i32().unwrap(),
				sandbox.to_bytes().as_ref(),
				runner.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Sandbox(crate::fdb::sandbox::Key::SandboxProcess { sandbox, process }) => (
				Kind::SandboxProcess.to_i32().unwrap(),
				sandbox.to_bytes().as_ref(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::fdb::process::Key::ProcessSandbox { process, sandbox }) => (
				Kind::ProcessSandbox.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				sandbox.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Sandbox(crate::fdb::sandbox::Key::CreatorSandbox { creator, sandbox }) => (
				Kind::CreatorSandbox.to_i32().unwrap(),
				creator.to_string(),
				sandbox.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Sandbox(crate::fdb::sandbox::Key::OwnerSandbox { owner, sandbox }) => (
				Kind::OwnerSandbox.to_i32().unwrap(),
				owner.to_string(),
				sandbox.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::fdb::tag::Key::Tag(id)) => {
				(Kind::Tag.to_i32().unwrap(), id.to_string()).pack(w, tuple_depth)
			},

			Key::Cache(crate::fdb::cache::Key::CacheEntryDependency {
				cache_entry,
				dependency,
			}) => (
				Kind::CacheEntryDependency.to_i32().unwrap(),
				cache_entry.to_bytes().as_ref(),
				dependency.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Cache(crate::fdb::cache::Key::DependencyCacheEntry {
				dependency,
				cache_entry,
			}) => (
				Kind::DependencyCacheEntry.to_i32().unwrap(),
				dependency.to_bytes().as_ref(),
				cache_entry.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::fdb::object::Key::ObjectChild { object, child }) => (
				Kind::ObjectChild.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::fdb::object::Key::ChildObject {
				child,
				object: parent,
			}) => (
				Kind::ChildObject.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::fdb::object::Key::ObjectCacheEntry {
				object,
				cache_entry,
			}) => (
				Kind::ObjectCacheEntry.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				cache_entry.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::fdb::object::Key::CacheEntryObject {
				cache_entry,
				object,
			}) => (
				Kind::CacheEntryObject.to_i32().unwrap(),
				cache_entry.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::fdb::process::Key::ProcessChild { process, child }) => (
				Kind::ProcessChild.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::fdb::process::Key::ChildProcess { child, parent }) => (
				Kind::ChildProcess.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::fdb::process::Key::ProcessObject {
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

			Key::Process(crate::fdb::process::Key::CommandCacheableProcess {
				command,
				process,
			}) => (
				Kind::CommandCacheableProcess.to_i32().unwrap(),
				command.to_bytes().as_ref(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::fdb::object::Key::ObjectProcess {
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

			Key::Tag(crate::fdb::tag::Key::ItemTag { item, tag }) => (
				Kind::ItemTag.to_i32().unwrap(),
				item.as_slice(),
				tag.to_string(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::fdb::tag::Key::ParentTag { parent, name, tag }) => (
				Kind::ParentTag.to_i32().unwrap(),
				parent.as_ref().map(ToString::to_string),
				name,
				tag.to_string(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::fdb::tag::Key::TagParent { tag, parent, name }) => (
				Kind::TagParent.to_i32().unwrap(),
				tag.to_string(),
				parent.as_ref().map(ToString::to_string),
				name,
			)
				.pack(w, tuple_depth),

			Key::User(crate::fdb::user::Key::User(user)) => (
				Kind::User.to_i32().unwrap(),
				tg::Id::from(user.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Group(crate::fdb::group::Key::Group(group)) => (
				Kind::Group.to_i32().unwrap(),
				tg::Id::from(group.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Organization(crate::fdb::organization::Key::Organization(organization)) => (
				Kind::Organization.to_i32().unwrap(),
				tg::Id::from(organization.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Group(crate::fdb::group::Key::GroupMember { group, member }) => (
				Kind::GroupMember.to_i32().unwrap(),
				tg::Id::from(group.clone()).to_bytes().as_ref(),
				tg::Id::from(member.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Group(crate::fdb::group::Key::MemberGroup { member, group }) => (
				Kind::MemberGroup.to_i32().unwrap(),
				tg::Id::from(member.clone()).to_bytes().as_ref(),
				tg::Id::from(group.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Organization(crate::fdb::organization::Key::OrganizationMember {
				organization,
				member,
			}) => (
				Kind::OrganizationMember.to_i32().unwrap(),
				tg::Id::from(organization.clone()).to_bytes().as_ref(),
				tg::Id::from(member.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Organization(crate::fdb::organization::Key::MemberOrganization {
				member,
				organization,
			}) => (
				Kind::MemberOrganization.to_i32().unwrap(),
				tg::Id::from(member.clone()).to_bytes().as_ref(),
				tg::Id::from(organization.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Grant(crate::fdb::grant::Key::ResourceGrant {
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

			Key::Grant(crate::fdb::grant::Key::PrincipalGrant {
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

			Key::Node(crate::fdb::node::Key::Node(specifier)) => {
				(Kind::Node.to_i32().unwrap(), specifier.to_string()).pack(w, tuple_depth)
			},

			Key::Grant(crate::fdb::grant::Key::Visibility {
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

			Key::Grant(crate::fdb::grant::Key::GrantExpiresAt {
				partition,
				expires_at,
				resource,
				principal,
				creator,
				permission,
				source,
			}) => (
				Kind::GrantExpiresAt.to_i32().unwrap(),
				partition,
				expires_at,
				resource.to_bytes().as_ref(),
				principal.to_string(),
				permission.to_string(),
				creator.as_ref().map(ToString::to_string),
				source.to_i32(),
			)
				.pack(w, tuple_depth),

			Key::Clean(crate::fdb::clean::Key::Clean {
				partition,
				touched_at,
				kind,
				id,
			}) => {
				Kind::Clean.to_i32().unwrap().pack(w, tuple_depth)?;
				partition.pack(w, tuple_depth)?;
				touched_at.pack(w, tuple_depth)?;
				kind.to_i32().unwrap().pack(w, tuple_depth)?;
				let id = id.to_bytes();
				id.as_ref().pack(w, tuple_depth)
			},

			Key::Finalization(crate::fdb::finalization::Key::Process(id)) => (
				Kind::ProcessFinalization.to_i32().unwrap(),
				id.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Finalization(crate::fdb::finalization::Key::ProcessVersion {
				id,
				partition,
				version,
			}) => (
				Kind::ProcessFinalizationVersion.to_i32().unwrap(),
				partition,
				version,
				id.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Finalization(crate::fdb::finalization::Key::Sandbox(id)) => (
				Kind::SandboxFinalization.to_i32().unwrap(),
				id.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Finalization(crate::fdb::finalization::Key::SandboxVersion {
				id,
				partition,
				version,
			}) => (
				Kind::SandboxFinalizationVersion.to_i32().unwrap(),
				partition,
				version,
				id.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Update(crate::fdb::update::Key::Update { id, kind }) => {
				Kind::Update.to_i32().unwrap().pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				let mut offset = id.as_ref().pack(w, tuple_depth)?;
				offset += pack_update_kind(w, tuple_depth, kind)?;
				Ok(offset)
			},

			Key::Update(crate::fdb::update::Key::UpdateVersion {
				id,
				kind,
				partition,
				version,
			}) => {
				let mut offset = Kind::UpdateVersion.to_i32().unwrap().pack(w, tuple_depth)?;
				offset += partition.pack(w, tuple_depth)?;
				offset += version.pack(w, tuple_depth)?;
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
		let (input, kind) = i32::unpack(input, tuple_depth)?;
		let kind = Kind::from_i32(kind).ok_or(fdbt::PackError::Message("invalid kind".into()))?;

		match kind {
			Kind::CacheEntry => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::artifact::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				Ok((input, Key::Cache(crate::fdb::cache::Key::CacheEntry(id))))
			},

			Kind::Object => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::object::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((input, Key::Object(crate::fdb::object::Key::Object(id))))
			},

			Kind::Process => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::process::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((input, Key::Process(crate::fdb::process::Key::Process(id))))
			},

			Kind::Sandbox => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::sandbox::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
				Ok((input, Key::Sandbox(crate::fdb::sandbox::Key::Sandbox(id))))
			},

			Kind::Runner => {
				let (input, bytes): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::runner::Id::from_slice(&bytes)
					.map_err(|_| fdbt::PackError::Message("invalid runner id".into()))?;
				Ok((input, Key::Runner(crate::fdb::runner::Key::Runner(id))))
			},

			Kind::SchedulerRunner => {
				let (input, scheduler): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, runner): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let scheduler = tg::scheduler::Id::from_slice(&scheduler)
					.map_err(|_| fdbt::PackError::Message("invalid scheduler id".into()))?;
				let runner = tg::runner::Id::from_slice(&runner)
					.map_err(|_| fdbt::PackError::Message("invalid runner id".into()))?;
				Ok((
					input,
					Key::Runner(crate::fdb::runner::Key::SchedulerRunner { scheduler, runner }),
				))
			},

			Kind::RunnerScheduler => {
				let (input, runner): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, scheduler): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let runner = tg::runner::Id::from_slice(&runner)
					.map_err(|_| fdbt::PackError::Message("invalid runner id".into()))?;
				let scheduler = tg::scheduler::Id::from_slice(&scheduler)
					.map_err(|_| fdbt::PackError::Message("invalid scheduler id".into()))?;
				Ok((
					input,
					Key::Runner(crate::fdb::runner::Key::RunnerScheduler { runner, scheduler }),
				))
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
					Key::Runner(crate::fdb::runner::Key::RunnerSandbox { runner, sandbox }),
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
					Key::Sandbox(crate::fdb::sandbox::Key::SandboxRunner { sandbox, runner }),
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
					Key::Sandbox(crate::fdb::sandbox::Key::SandboxProcess { sandbox, process }),
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
					Key::Process(crate::fdb::process::Key::ProcessSandbox { process, sandbox }),
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
					Key::Sandbox(crate::fdb::sandbox::Key::CreatorSandbox { creator, sandbox }),
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
					Key::Sandbox(crate::fdb::sandbox::Key::OwnerSandbox { owner, sandbox }),
				))
			},

			Kind::Tag => {
				let (input, id): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = id
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid tag id".into()))?;
				Ok((input, Key::Tag(crate::fdb::tag::Key::Tag(id))))
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
				let key = Key::Cache(crate::fdb::cache::Key::CacheEntryDependency {
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
				let key = Key::Cache(crate::fdb::cache::Key::DependencyCacheEntry {
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
					Key::Object(crate::fdb::object::Key::ObjectChild { object, child }),
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
					Key::Object(crate::fdb::object::Key::ChildObject { child, object }),
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
				let key = Key::Object(crate::fdb::object::Key::ObjectCacheEntry {
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
				let key = Key::Object(crate::fdb::object::Key::CacheEntryObject {
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
					Key::Process(crate::fdb::process::Key::ProcessChild { process, child }),
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
					Key::Process(crate::fdb::process::Key::ChildProcess { child, parent }),
				))
			},

			Kind::ProcessObject => {
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind) = crate::process::object::Kind::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::Process(crate::fdb::process::Key::ProcessObject {
					process,
					kind,
					object,
				});
				Ok((input, key))
			},

			Kind::ObjectProcess => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind) = crate::process::object::Kind::unpack(input, tuple_depth)?;
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::Object(crate::fdb::object::Key::ObjectProcess {
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
				let key = Key::Process(crate::fdb::process::Key::CommandCacheableProcess {
					command,
					process,
				});
				Ok((input, key))
			},

			Kind::ItemTag => {
				let (input, item): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let tag = tag
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid tag id".into()))?;
				Ok((input, Key::Tag(crate::fdb::tag::Key::ItemTag { item, tag })))
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
					Key::Tag(crate::fdb::tag::Key::ParentTag { parent, name, tag }),
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
					Key::Tag(crate::fdb::tag::Key::TagParent { tag, parent, name }),
				))
			},

			Kind::User => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid user id".into()))?;
				let id = tg::user::Id::try_from(id)
					.map_err(|_| fdbt::PackError::Message("invalid user id".into()))?;
				Ok((input, Key::User(crate::fdb::user::Key::User(id))))
			},

			Kind::Group => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid group id".into()))?;
				let id = tg::group::Id::try_from(id)
					.map_err(|_| fdbt::PackError::Message("invalid group id".into()))?;
				Ok((input, Key::Group(crate::fdb::group::Key::Group(id))))
			},

			Kind::Organization => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid organization id".into()))?;
				let id = tg::organization::Id::try_from(id)
					.map_err(|_| fdbt::PackError::Message("invalid organization id".into()))?;
				let key = Key::Organization(crate::fdb::organization::Key::Organization(id));
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
				let key = Key::Group(crate::fdb::group::Key::GroupMember { group, member });
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
				let key = Key::Group(crate::fdb::group::Key::MemberGroup { member, group });
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
				let key = Key::Organization(crate::fdb::organization::Key::OrganizationMember {
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
				let key = Key::Organization(crate::fdb::organization::Key::MemberOrganization {
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
				let key = Key::Grant(crate::fdb::grant::Key::ResourceGrant {
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
				let key = Key::Grant(crate::fdb::grant::Key::PrincipalGrant {
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
				Ok((input, Key::Node(crate::fdb::node::Key::Node(specifier))))
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
				let key = Key::Grant(crate::fdb::grant::Key::Visibility {
					resource,
					principal,
					grant_resource,
					creator,
					permission,
				});
				Ok((input, key))
			},

			Kind::GrantExpiresAt => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
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
				let source = crate::fdb::grant::GrantSource::from_i32(source)
					.ok_or_else(|| fdbt::PackError::Message("invalid grant source".into()))?;
				let key = Key::Grant(crate::fdb::grant::Key::GrantExpiresAt {
					partition,
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
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, touched_at): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let kind = crate::fdb::clean::ItemKind::from_i32(kind)
					.ok_or(fdbt::PackError::Message("invalid item kind".into()))?;
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let key = Key::Clean(crate::fdb::clean::Key::Clean {
					partition,
					touched_at,
					kind,
					id,
				});
				Ok((input, key))
			},

			Kind::ProcessFinalization => {
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::process::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((
					input,
					Key::Finalization(crate::fdb::finalization::Key::Process(id)),
				))
			},

			Kind::ProcessFinalizationVersion => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, version) = fdbt::Versionstamp::unpack(input, tuple_depth)?;
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::process::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::Finalization(crate::fdb::finalization::Key::ProcessVersion {
					id,
					partition,
					version,
				});
				Ok((input, key))
			},

			Kind::SandboxFinalization => {
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::sandbox::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
				Ok((
					input,
					Key::Finalization(crate::fdb::finalization::Key::Sandbox(id)),
				))
			},

			Kind::SandboxFinalizationVersion => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, version) = fdbt::Versionstamp::unpack(input, tuple_depth)?;
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::sandbox::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
				let key = Key::Finalization(crate::fdb::finalization::Key::SandboxVersion {
					id,
					partition,
					version,
				});
				Ok((input, key))
			},

			Kind::Update => {
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
				let (input, kind) = unpack_update_kind(input, tuple_depth)?;
				Ok((
					input,
					Key::Update(crate::fdb::update::Key::Update { id, kind }),
				))
			},

			Kind::UpdateVersion => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, version) = fdbt::Versionstamp::unpack(input, tuple_depth)?;
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
				let (input, kind) = unpack_update_kind(input, tuple_depth)?;
				let key = Key::Update(crate::fdb::update::Key::UpdateVersion {
					id,
					kind,
					partition,
					version,
				});
				Ok((input, key))
			},
		}
	}
}

fn pack_update_kind<W: std::io::Write>(
	w: &mut W,
	tuple_depth: fdbt::TupleDepth,
	kind: &crate::fdb::update::Kind,
) -> std::io::Result<fdbt::VersionstampOffset> {
	match kind {
		crate::fdb::update::Kind::Item => 0i32.pack(w, tuple_depth),
		crate::fdb::update::Kind::Grants(principal) => {
			let mut offset = 1i32.pack(w, tuple_depth)?;
			offset += principal.to_string().pack(w, tuple_depth)?;
			Ok(offset)
		},
	}
}

fn unpack_update_kind(
	input: &[u8],
	tuple_depth: fdbt::TupleDepth,
) -> Result<(&[u8], crate::fdb::update::Kind), fdbt::PackError> {
	let (input, kind): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
	match kind {
		0 => Ok((input, crate::fdb::update::Kind::Item)),
		1 => {
			let (input, principal): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
			let principal = principal
				.parse()
				.map_err(|_| fdbt::PackError::Message("invalid grant principal".into()))?;
			Ok((input, crate::fdb::update::Kind::Grants(principal)))
		},
		_ => Err(fdbt::PackError::Message("invalid update kind".into())),
	}
}

impl fdbt::TuplePack for Kind {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		self.to_i32().unwrap().pack(w, tuple_depth)
	}
}

impl fdbt::TupleUnpack<'_> for Kind {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, value) = i32::unpack(input, tuple_depth)?;
		let kind = Self::from_i32(value).ok_or(fdbt::PackError::Message("invalid kind".into()))?;
		Ok((input, kind))
	}
}
