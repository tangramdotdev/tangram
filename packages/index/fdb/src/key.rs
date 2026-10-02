use {
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub enum Key {
	Checkout(crate::checkout::Key),
	Clean(crate::clean::Key),
	Delegation(crate::delegation::Key),
	Permission(crate::permission::Key),
	PermissionCapture { id: Vec<u8>, partition: u64 },
	Group(crate::group::Key),
	Indexer(crate::indexer::Key),
	LogCompaction(crate::log::Key),
	Node(crate::node::Key),
	Object(crate::object::Key),
	Organization(crate::organization::Key),
	Process(crate::process::Key),
	Runner(crate::runner::Key),
	Sandbox(crate::sandbox::Key),
	Tag(crate::tag::Key),
	Update(crate::update::Key),
	Usage(crate::usage::Key),
	User(crate::user::Key),
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
pub enum Kind {
	Checkout = 0,
	Object = 1,
	Process = 2,
	Tag = 3,
	CheckoutDependency = 4,
	DependencyCheckout = 5,
	ObjectChild = 6,
	ChildObject = 7,
	ObjectCheckout = 8,
	CheckoutObject = 9,
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
	ResourcePermission = 27,
	SubjectPermission = 28,
	Node = 29,
	Visibility = 30,
	PermissionExpiresAt = 31,
	Sandbox = 32,
	CommandCacheableProcess = 33,
	RunnerSandbox = 38,
	SandboxRunner = 39,
	SandboxProcess = 40,
	ProcessSandbox = 41,
	CreatorSandbox = 42,
	OwnerSandbox = 43,
	LogCompaction = 45,
	LogCompactionVersion = 47,
	AccountObject = 51,
	ObjectAccount = 52,
	AccountProcess = 53,
	ProcessAccount = 54,
	UsageAggregate = 55,
	UsageDelta = 56,
	UsageAggregation = 57,
	PermissionUpdate = 58,
	PermissionUpdateVersion = 59,
	StorageAndMetadataUpdate = 60,
	StorageAndMetadataUpdateVersion = 61,
	UsageUpdate = 62,
	UsageUpdateVersion = 63,
	UsageStarted = 64,
	UsageUnavailable = 65,
	PermissionUpdatePropagatedVersion = 66,
	StorageAndMetadataUpdatePropagatedVersion = 67,
	UsageUpdatePropagatedVersion = 69,
	UsageUpdatePutVersion = 68,
	Indexer = 70,
	PermissionUpdateClean = 71,
	StorageAndMetadataUpdateClean = 72,
	Delegation = 73,
	DelegationSource = 74,
	DelegationExpiresAt = 75,
	DelegationSubject = 76,
	PermissionCapture = 90,
}

impl fdbt::TuplePack for Key {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			Key::PermissionCapture { id, partition } => (
				Kind::PermissionCapture.to_i32().unwrap(),
				*partition,
				id.as_slice(),
			)
				.pack(w, tuple_depth),
			Key::Delegation(crate::delegation::Key::Delegation {
				resource,
				source,
				subject,
			}) => (
				Kind::Delegation.to_i32().unwrap(),
				resource.to_bytes().as_ref(),
				subject.to_string(),
				source.to_string(),
			)
				.pack(w, tuple_depth),
			Key::Delegation(crate::delegation::Key::Subject {
				resource,
				source,
				subject,
			}) => (
				Kind::DelegationSubject.to_i32().unwrap(),
				subject.to_string(),
				resource.to_bytes().as_ref(),
				source.to_string(),
			)
				.pack(w, tuple_depth),

			Key::Delegation(crate::delegation::Key::Source {
				resource,
				source,
				subject,
			}) => (
				Kind::DelegationSource.to_i32().unwrap(),
				resource.to_bytes().as_ref(),
				source.to_string(),
				subject.to_string(),
			)
				.pack(w, tuple_depth),
			Key::Delegation(crate::delegation::Key::ExpiresAt {
				expires_at,
				resource,
				source,
				subject,
			}) => (
				Kind::DelegationExpiresAt.to_i32().unwrap(),
				expires_at,
				resource.to_bytes().as_ref(),
				subject.to_string(),
				source.to_string(),
			)
				.pack(w, tuple_depth),

			Key::Indexer(crate::indexer::Key::Indexer(id)) => {
				(Kind::Indexer.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},
			Key::Usage(crate::usage::Key::AccountObject { account, object }) => (
				Kind::AccountObject.to_i32().unwrap(),
				account.id().to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),
			Key::Usage(crate::usage::Key::ObjectAccount { account, object }) => (
				Kind::ObjectAccount.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				account.id().to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Usage(crate::usage::Key::AccountProcess { account, process }) => (
				Kind::AccountProcess.to_i32().unwrap(),
				account.id().to_bytes().as_ref(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),
			Key::Usage(crate::usage::Key::ProcessAccount { account, process }) => (
				Kind::ProcessAccount.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				account.id().to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Usage(crate::usage::Key::Aggregate {
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
			Key::Usage(crate::usage::Key::Aggregation {
				account,
				hour,
				partition,
			}) => (
				Kind::UsageAggregation.to_i32().unwrap(),
				partition,
				hour,
				account.id().to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),
			Key::Usage(crate::usage::Key::Delta {
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
			Key::Usage(crate::usage::Key::Started) => {
				Kind::UsageStarted.to_i32().unwrap().pack(w, tuple_depth)
			},
			Key::Usage(crate::usage::Key::Unavailable {
				account,
				kind,
				partition,
			}) => (
				Kind::UsageUnavailable.to_i32().unwrap(),
				partition,
				i32::from(*kind as u8),
				account.id().to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Checkout(crate::checkout::Key::Checkout(id)) => {
				(Kind::Checkout.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Object(crate::object::Key::Object(id)) => {
				(Kind::Object.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Process(crate::process::Key::Process(id)) => {
				(Kind::Process.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Sandbox(crate::sandbox::Key::Sandbox(id)) => {
				(Kind::Sandbox.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Runner(crate::runner::Key::RunnerSandbox { runner, sandbox }) => (
				Kind::RunnerSandbox.to_i32().unwrap(),
				runner.to_bytes().as_ref(),
				sandbox.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Sandbox(crate::sandbox::Key::SandboxRunner { sandbox, runner }) => (
				Kind::SandboxRunner.to_i32().unwrap(),
				sandbox.to_bytes().as_ref(),
				runner.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Sandbox(crate::sandbox::Key::SandboxProcess {
				position,
				process,
				sandbox,
			}) => (
				Kind::SandboxProcess.to_i32().unwrap(),
				sandbox.to_bytes().as_ref(),
				*position,
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),
			Key::Process(crate::process::Key::ProcessSandbox { process, sandbox }) => (
				Kind::ProcessSandbox.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				sandbox.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Sandbox(crate::sandbox::Key::CreatorSandbox { creator, sandbox }) => (
				Kind::CreatorSandbox.to_i32().unwrap(),
				creator.to_string(),
				sandbox.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Sandbox(crate::sandbox::Key::OwnerSandbox { owner, sandbox }) => (
				Kind::OwnerSandbox.to_i32().unwrap(),
				owner.to_string(),
				sandbox.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::tag::Key::Tag(id)) => {
				(Kind::Tag.to_i32().unwrap(), id.to_string()).pack(w, tuple_depth)
			},

			Key::Checkout(crate::checkout::Key::CheckoutDependency {
				checkout,
				dependency,
			}) => (
				Kind::CheckoutDependency.to_i32().unwrap(),
				checkout.to_bytes().as_ref(),
				dependency.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Checkout(crate::checkout::Key::DependencyCheckout {
				checkout,
				dependency,
			}) => (
				Kind::DependencyCheckout.to_i32().unwrap(),
				dependency.to_bytes().as_ref(),
				checkout.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::object::Key::ObjectChild { object, child }) => (
				Kind::ObjectChild.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::object::Key::ChildObject {
				child,
				object: parent,
			}) => (
				Kind::ChildObject.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::object::Key::ObjectCheckout { object, checkout }) => (
				Kind::ObjectCheckout.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				checkout.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::object::Key::CheckoutObject { checkout, object }) => (
				Kind::CheckoutObject.to_i32().unwrap(),
				checkout.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::process::Key::ProcessChild {
				child,
				position,
				process,
			}) => (
				Kind::ProcessChild.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				position,
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::process::Key::ChildProcess { child, parent }) => (
				Kind::ChildProcess.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::process::Key::ProcessObject {
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

			Key::Process(crate::process::Key::CommandCacheableProcess { command, process }) => (
				Kind::CommandCacheableProcess.to_i32().unwrap(),
				command.to_bytes().as_ref(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::object::Key::ObjectProcess {
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

			Key::Tag(crate::tag::Key::TargetTag { target, tag }) => (
				Kind::TargetTag.to_i32().unwrap(),
				target.as_slice(),
				tag.to_string(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::tag::Key::ParentTag { parent, name, tag }) => (
				Kind::ParentTag.to_i32().unwrap(),
				parent.as_ref().map(ToString::to_string),
				name,
				tag.to_string(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::tag::Key::TagParent { tag, parent, name }) => (
				Kind::TagParent.to_i32().unwrap(),
				tag.to_string(),
				parent.as_ref().map(ToString::to_string),
				name,
			)
				.pack(w, tuple_depth),

			Key::User(crate::user::Key::User(user)) => (
				Kind::User.to_i32().unwrap(),
				tg::Id::from(user.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Group(crate::group::Key::Group(group)) => (
				Kind::Group.to_i32().unwrap(),
				tg::Id::from(group.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Organization(crate::organization::Key::Organization(organization)) => (
				Kind::Organization.to_i32().unwrap(),
				tg::Id::from(organization.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Group(crate::group::Key::GroupMember { group, member }) => (
				Kind::GroupMember.to_i32().unwrap(),
				tg::Id::from(group.clone()).to_bytes().as_ref(),
				tg::Id::from(member.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Group(crate::group::Key::MemberGroup { member, group }) => (
				Kind::MemberGroup.to_i32().unwrap(),
				tg::Id::from(member.clone()).to_bytes().as_ref(),
				tg::Id::from(group.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Organization(crate::organization::Key::OrganizationMember {
				organization,
				member,
			}) => (
				Kind::OrganizationMember.to_i32().unwrap(),
				tg::Id::from(organization.clone()).to_bytes().as_ref(),
				tg::Id::from(member.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Organization(crate::organization::Key::MemberOrganization {
				member,
				organization,
			}) => (
				Kind::MemberOrganization.to_i32().unwrap(),
				tg::Id::from(member.clone()).to_bytes().as_ref(),
				tg::Id::from(organization.clone()).to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Permission(crate::permission::Key::ResourcePermission {
				resource,
				subject,
				creator,
				permission,
			}) => (
				Kind::ResourcePermission.to_i32().unwrap(),
				resource.to_bytes().as_ref(),
				subject.to_string(),
				permission.to_string(),
				creator.as_ref().map(ToString::to_string),
			)
				.pack(w, tuple_depth),

			Key::Permission(crate::permission::Key::SubjectPermission {
				subject,
				resource,
				creator,
				permission,
			}) => (
				Kind::SubjectPermission.to_i32().unwrap(),
				subject.to_string(),
				resource.to_bytes().as_ref(),
				permission.to_string(),
				creator.as_ref().map(ToString::to_string),
			)
				.pack(w, tuple_depth),

			Key::Node(crate::node::Key::Node(specifier)) => {
				(Kind::Node.to_i32().unwrap(), specifier.to_string()).pack(w, tuple_depth)
			},

			Key::Permission(crate::permission::Key::Visibility {
				resource,
				subject,
				permission_resource,
				creator,
				permission,
			}) => (
				Kind::Visibility.to_i32().unwrap(),
				resource.to_bytes().as_ref(),
				subject.to_string(),
				permission_resource.to_bytes().as_ref(),
				permission.to_string(),
				creator.as_ref().map(ToString::to_string),
			)
				.pack(w, tuple_depth),

			Key::Permission(crate::permission::Key::PermissionExpiresAt {
				partition,
				expires_at,
				resource,
				subject,
				creator,
				permission,
				source,
			}) => (
				Kind::PermissionExpiresAt.to_i32().unwrap(),
				partition,
				expires_at,
				resource.to_bytes().as_ref(),
				subject.to_string(),
				permission.to_string(),
				creator.as_ref().map(ToString::to_string),
				source.to_i32(),
			)
				.pack(w, tuple_depth),

			Key::Clean(key) => {
				Kind::Clean.to_i32().unwrap().pack(w, tuple_depth)?;
				match key {
					crate::clean::Key::AccountObject {
						account,
						object,
						partition,
						touched_at,
					} => (
						partition,
						touched_at,
						crate::clean::ItemKind::AccountObject.to_i32().unwrap(),
						account.id().to_bytes().as_ref(),
						object.to_bytes().as_ref(),
					)
						.pack(w, tuple_depth),
					crate::clean::Key::AccountProcess {
						account,
						partition,
						process,
						touched_at,
					} => (
						partition,
						touched_at,
						crate::clean::ItemKind::AccountProcess.to_i32().unwrap(),
						account.id().to_bytes().as_ref(),
						process.to_bytes().as_ref(),
					)
						.pack(w, tuple_depth),
					crate::clean::Key::Checkout {
						id,
						partition,
						touched_at,
					} => (
						partition,
						touched_at,
						crate::clean::ItemKind::Checkout.to_i32().unwrap(),
						id.to_bytes().as_ref(),
					)
						.pack(w, tuple_depth),
					crate::clean::Key::Object {
						id,
						partition,
						touched_at,
					} => (
						partition,
						touched_at,
						crate::clean::ItemKind::Object.to_i32().unwrap(),
						id.to_bytes().as_ref(),
					)
						.pack(w, tuple_depth),
					crate::clean::Key::Process {
						id,
						partition,
						touched_at,
					} => (
						partition,
						touched_at,
						crate::clean::ItemKind::Process.to_i32().unwrap(),
						id.to_bytes().as_ref(),
					)
						.pack(w, tuple_depth),
					crate::clean::Key::Sandbox {
						id,
						partition,
						touched_at,
					} => (
						partition,
						touched_at,
						crate::clean::ItemKind::Sandbox.to_i32().unwrap(),
						id.to_bytes().as_ref(),
					)
						.pack(w, tuple_depth),
				}
			},

			Key::LogCompaction(crate::log::Key::Identity(process)) => (
				Kind::LogCompaction.to_i32().unwrap(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::LogCompaction(crate::log::Key::Version {
				partition,
				process,
				version,
			}) => (
				Kind::LogCompactionVersion.to_i32().unwrap(),
				partition,
				version,
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Update(crate::update::Key::PropagatedVersion { id, kind }) => {
				let key_kind = match kind {
					crate::update::Kind::Permission(_) => Kind::PermissionUpdatePropagatedVersion,
					crate::update::Kind::StorageAndMetadata => {
						Kind::StorageAndMetadataUpdatePropagatedVersion
					},
					crate::update::Kind::Usage(_) => unreachable!(),
				};
				key_kind.to_i32().unwrap().pack(w, tuple_depth)?;
				let id = match id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				let mut offset = id.as_ref().pack(w, tuple_depth)?;
				offset += pack_update_kind(w, tuple_depth, kind)?;
				Ok(offset)
			},

			Key::Update(
				crate::update::Key::UsageUpdatePropagatedVersion { account, id }
				| crate::update::Key::UsageUpdatePutVersion { account, id },
			) => {
				let kind = match self {
					Key::Update(crate::update::Key::UsageUpdatePropagatedVersion { .. }) => {
						Kind::UsageUpdatePropagatedVersion
					},
					_ => Kind::UsageUpdatePutVersion,
				};
				let id = match id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				(
					kind.to_i32().unwrap(),
					id.as_ref(),
					account.id().to_bytes().as_ref(),
				)
					.pack(w, tuple_depth)
			},

			Key::Update(crate::update::Key::Update { id, kind }) => {
				let key_kind = match kind {
					crate::update::Kind::Permission(_) => Kind::PermissionUpdate,
					crate::update::Kind::StorageAndMetadata => Kind::StorageAndMetadataUpdate,
					crate::update::Kind::Usage(_) => Kind::UsageUpdate,
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

			Key::Update(
				crate::update::Key::UpdateVersion {
					id,
					kind,
					partition,
					version,
				}
				| crate::update::Key::Clean {
					id,
					kind,
					partition,
					version,
				},
			) => {
				let clean = matches!(self, Key::Update(crate::update::Key::Clean { .. }));
				let key_kind = match kind {
					crate::update::Kind::Permission(_) if clean => Kind::PermissionUpdateClean,
					crate::update::Kind::Permission(_) => Kind::PermissionUpdateVersion,
					crate::update::Kind::StorageAndMetadata if clean => {
						Kind::StorageAndMetadataUpdateClean
					},
					crate::update::Kind::StorageAndMetadata => {
						Kind::StorageAndMetadataUpdateVersion
					},
					crate::update::Kind::Usage(_) => Kind::UsageUpdateVersion,
				};
				let mut offset = key_kind.to_i32().unwrap().pack(w, tuple_depth)?;
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
			Kind::PermissionCapture => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				Ok((input, Key::PermissionCapture { id, partition }))
			},
			Kind::DelegationSubject => {
				let (input, subject): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, resource): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, source): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let resource = tg::Id::from_slice(&resource).map_err(|_| {
					fdbt::PackError::Message("invalid delegation resource id".into())
				})?;
				let subject = subject
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid subject".into()))?;
				let source = source
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid subject".into()))?;
				let key = crate::delegation::Key::Subject {
					resource,
					source,
					subject,
				};
				Ok((input, Key::Delegation(key)))
			},

			Kind::Delegation | Kind::DelegationSource | Kind::DelegationExpiresAt => {
				let (input, expires_at) = if kind == Kind::DelegationExpiresAt {
					let (input, expires_at): (_, i64) =
						fdbt::TupleUnpack::unpack(input, tuple_depth)?;
					(input, Some(expires_at))
				} else {
					(input, None)
				};
				let (input, resource): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, first): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, second): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let resource = tg::Id::from_slice(&resource).map_err(|_| {
					fdbt::PackError::Message("invalid delegation resource id".into())
				})?;
				let first = first
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid subject".into()))?;
				let second = second
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid subject".into()))?;
				let key = match kind {
					Kind::Delegation => crate::delegation::Key::Delegation {
						resource,
						source: second,
						subject: first,
					},
					Kind::DelegationSource => crate::delegation::Key::Source {
						resource,
						source: first,
						subject: second,
					},
					Kind::DelegationExpiresAt => crate::delegation::Key::ExpiresAt {
						expires_at: expires_at.unwrap(),
						resource,
						source: second,
						subject: first,
					},
					_ => unreachable!(),
				};
				Ok((input, Key::Delegation(key)))
			},

			Kind::Indexer => {
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::indexer::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid indexer id".into()))?;
				Ok((input, Key::Indexer(crate::indexer::Key::Indexer(id))))
			},
			Kind::AccountObject => {
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, object): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = tangram_index::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let object = tg::object::Id::from_slice(&object)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::Usage(crate::usage::Key::AccountObject { account, object });
				Ok((input, key))
			},
			Kind::ObjectAccount => {
				let (input, object): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = tangram_index::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let key = Key::Usage(crate::usage::Key::ObjectAccount { account, object });
				Ok((input, key))
			},

			Kind::AccountProcess => {
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, process): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = tangram_index::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let process = tg::process::Id::from_slice(&process)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::Usage(crate::usage::Key::AccountProcess { account, process });
				Ok((input, key))
			},
			Kind::ProcessAccount => {
				let (input, process): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = tangram_index::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let key = Key::Usage(crate::usage::Key::ProcessAccount { account, process });
				Ok((input, key))
			},

			Kind::UsageAggregate => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, period_kind): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, start): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = tangram_index::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let kind = match period_kind {
					0 => tangram_index::usage::PeriodKind::Hour,
					1 => tangram_index::usage::PeriodKind::Day,
					2 => tangram_index::usage::PeriodKind::Week,
					3 => tangram_index::usage::PeriodKind::Month,
					_ => return Err(fdbt::PackError::Message("invalid usage period kind".into())),
				};
				let period = tangram_index::usage::Period::from_kind_and_start(kind, start)
					.map_err(|_| fdbt::PackError::Message("invalid usage period".into()))?;
				let key = Key::Usage(crate::usage::Key::Aggregate {
					account,
					partition,
					period,
				});
				Ok((input, key))
			},
			Kind::UsageAggregation => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, hour): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = tangram_index::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let key = Key::Usage(crate::usage::Key::Aggregation {
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
				let account = tangram_index::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let kind = tangram_index::usage::DeltaKind::from_i32(delta_kind)
					.ok_or_else(|| fdbt::PackError::Message("invalid usage delta kind".into()))?;
				let key = Key::Usage(crate::usage::Key::Delta {
					account,
					hour,
					kind,
					partition,
				});
				Ok((input, key))
			},
			Kind::UsageStarted => Ok((input, Key::Usage(crate::usage::Key::Started))),
			Kind::UsageUnavailable => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, period_kind): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = tangram_index::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let kind = match period_kind {
					0 => tangram_index::usage::PeriodKind::Hour,
					1 => tangram_index::usage::PeriodKind::Day,
					2 => tangram_index::usage::PeriodKind::Week,
					3 => tangram_index::usage::PeriodKind::Month,
					_ => return Err(fdbt::PackError::Message("invalid usage period kind".into())),
				};
				let key = Key::Usage(crate::usage::Key::Unavailable {
					account,
					kind,
					partition,
				});
				Ok((input, key))
			},

			Kind::Checkout => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				Ok((input, Key::Checkout(crate::checkout::Key::Checkout(id))))
			},

			Kind::Object => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::object::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((input, Key::Object(crate::object::Key::Object(id))))
			},

			Kind::Process => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::process::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((input, Key::Process(crate::process::Key::Process(id))))
			},

			Kind::Sandbox => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::sandbox::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
				Ok((input, Key::Sandbox(crate::sandbox::Key::Sandbox(id))))
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
					Key::Runner(crate::runner::Key::RunnerSandbox { runner, sandbox }),
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
					Key::Sandbox(crate::sandbox::Key::SandboxRunner { sandbox, runner }),
				))
			},

			Kind::SandboxProcess => {
				let (input, sandbox): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, position): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, process): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let sandbox = tg::sandbox::Id::from_slice(&sandbox)
					.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
				let process = tg::process::Id::from_slice(&process)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((
					input,
					Key::Sandbox(crate::sandbox::Key::SandboxProcess {
						position,
						process,
						sandbox,
					}),
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
					Key::Process(crate::process::Key::ProcessSandbox { process, sandbox }),
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
					Key::Sandbox(crate::sandbox::Key::CreatorSandbox { creator, sandbox }),
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
					Key::Sandbox(crate::sandbox::Key::OwnerSandbox { owner, sandbox }),
				))
			},

			Kind::Tag => {
				let (input, id): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = id
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid tag id".into()))?;
				Ok((input, Key::Tag(crate::tag::Key::Tag(id))))
			},

			Kind::CheckoutDependency => {
				let (input, checkout_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, dependency_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let checkout = tg::Id::from_slice(&checkout_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let dependency = tg::Id::from_slice(&dependency_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let key = Key::Checkout(crate::checkout::Key::CheckoutDependency {
					checkout,
					dependency,
				});
				Ok((input, key))
			},

			Kind::DependencyCheckout => {
				let (input, dependency_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, checkout_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let dependency = tg::Id::from_slice(&dependency_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let checkout = tg::Id::from_slice(&checkout_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let key = Key::Checkout(crate::checkout::Key::DependencyCheckout {
					checkout,
					dependency,
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
					Key::Object(crate::object::Key::ObjectChild { object, child }),
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
					Key::Object(crate::object::Key::ChildObject { child, object }),
				))
			},

			Kind::ObjectCheckout => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, checkout_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let checkout = tg::artifact::Id::from_slice(&checkout_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::Object(crate::object::Key::ObjectCheckout { object, checkout });
				Ok((input, key))
			},

			Kind::CheckoutObject => {
				let (input, checkout_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let checkout = tg::artifact::Id::from_slice(&checkout_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::Object(crate::object::Key::CheckoutObject { checkout, object });
				Ok((input, key))
			},

			Kind::ProcessChild => {
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, position): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let child = tg::process::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((
					input,
					Key::Process(crate::process::Key::ProcessChild {
						child,
						position,
						process,
					}),
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
					Key::Process(crate::process::Key::ChildProcess { child, parent }),
				))
			},

			Kind::ProcessObject => {
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind) =
					tangram_index::process::object::Kind::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::Process(crate::process::Key::ProcessObject {
					process,
					kind,
					object,
				});
				Ok((input, key))
			},

			Kind::ObjectProcess => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind) =
					tangram_index::process::object::Kind::unpack(input, tuple_depth)?;
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::Object(crate::object::Key::ObjectProcess {
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
				let key =
					Key::Process(crate::process::Key::CommandCacheableProcess { command, process });
				Ok((input, key))
			},

			Kind::TargetTag => {
				let (input, target): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let tag = tag
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid tag id".into()))?;
				Ok((input, Key::Tag(crate::tag::Key::TargetTag { target, tag })))
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
					Key::Tag(crate::tag::Key::ParentTag { parent, name, tag }),
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
					Key::Tag(crate::tag::Key::TagParent { tag, parent, name }),
				))
			},

			Kind::User => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid user id".into()))?;
				let id = tg::user::Id::try_from(id)
					.map_err(|_| fdbt::PackError::Message("invalid user id".into()))?;
				Ok((input, Key::User(crate::user::Key::User(id))))
			},

			Kind::Group => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid group id".into()))?;
				let id = tg::group::Id::try_from(id)
					.map_err(|_| fdbt::PackError::Message("invalid group id".into()))?;
				Ok((input, Key::Group(crate::group::Key::Group(id))))
			},

			Kind::Organization => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid organization id".into()))?;
				let id = tg::organization::Id::try_from(id)
					.map_err(|_| fdbt::PackError::Message("invalid organization id".into()))?;
				let key = Key::Organization(crate::organization::Key::Organization(id));
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
				let key = Key::Group(crate::group::Key::GroupMember { group, member });
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
				let key = Key::Group(crate::group::Key::MemberGroup { member, group });
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
				let key = Key::Organization(crate::organization::Key::OrganizationMember {
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
				let key = Key::Organization(crate::organization::Key::MemberOrganization {
					member,
					organization,
				});
				Ok((input, key))
			},

			Kind::ResourcePermission => {
				let (input, resource_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, subject): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, permission): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, creator): (_, Option<String>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let resource = tg::Id::from_slice(&resource_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid resource id".into()))?;
				let subject = subject
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid verification subject".into()))?;
				let creator = creator
					.map(|creator| {
						creator.parse().map_err(|_| {
							fdbt::PackError::Message("invalid permission creator".into())
						})
					})
					.transpose()?;
				let permission = permission.parse().map_err(|_| {
					fdbt::PackError::Message("invalid verification permission".into())
				})?;
				let key = Key::Permission(crate::permission::Key::ResourcePermission {
					resource,
					subject,
					creator,
					permission,
				});
				Ok((input, key))
			},

			Kind::SubjectPermission => {
				let (input, subject): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, resource_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, permission): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, creator): (_, Option<String>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let subject = subject
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid verification subject".into()))?;
				let resource = tg::Id::from_slice(&resource_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid resource id".into()))?;
				let creator = creator
					.map(|creator| {
						creator.parse().map_err(|_| {
							fdbt::PackError::Message("invalid permission creator".into())
						})
					})
					.transpose()?;
				let permission = permission.parse().map_err(|_| {
					fdbt::PackError::Message("invalid verification permission".into())
				})?;
				let key = Key::Permission(crate::permission::Key::SubjectPermission {
					subject,
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
				Ok((input, Key::Node(crate::node::Key::Node(specifier))))
			},

			Kind::Visibility => {
				let (input, resource_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, subject): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, permission_resource_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, permission): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, creator): (_, Option<String>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let resource = tg::Id::from_slice(&resource_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid resource id".into()))?;
				let subject = subject
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid verification subject".into()))?;
				let permission_resource = tg::Id::from_slice(&permission_resource_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid resource id".into()))?;
				let creator = creator
					.map(|creator| {
						creator.parse().map_err(|_| {
							fdbt::PackError::Message("invalid permission creator".into())
						})
					})
					.transpose()?;
				let permission = permission.parse().map_err(|_| {
					fdbt::PackError::Message("invalid verification permission".into())
				})?;
				let key = Key::Permission(crate::permission::Key::Visibility {
					resource,
					subject,
					permission_resource,
					creator,
					permission,
				});
				Ok((input, key))
			},

			Kind::PermissionExpiresAt => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, expires_at): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, resource_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, subject): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, permission): (_, String) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, creator): (_, Option<String>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, source): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let resource = tg::Id::from_slice(&resource_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid resource id".into()))?;
				let subject = subject
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid verification subject".into()))?;
				let creator = creator
					.map(|creator| {
						creator.parse().map_err(|_| {
							fdbt::PackError::Message("invalid permission creator".into())
						})
					})
					.transpose()?;
				let permission = permission.parse().map_err(|_| {
					fdbt::PackError::Message("invalid verification permission".into())
				})?;
				let source = crate::permission::PermissionSource::from_i32(source)
					.ok_or_else(|| fdbt::PackError::Message("invalid permission source".into()))?;
				let key = Key::Permission(crate::permission::Key::PermissionExpiresAt {
					partition,
					expires_at,
					resource,
					subject,
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
				let kind = crate::clean::ItemKind::from_i32(kind)
					.ok_or(fdbt::PackError::Message("invalid cleanup item kind".into()))?;
				let (input, key) = match kind {
					crate::clean::ItemKind::AccountObject => {
						let (input, account): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let (input, object): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let account = tg::Id::from_slice(&account).map_err(|_| {
							fdbt::PackError::Message("invalid usage account".into())
						})?;
						let account =
							tangram_index::usage::Account::try_from(account).map_err(|_| {
								fdbt::PackError::Message("invalid usage account".into())
							})?;
						let object = tg::object::Id::from_slice(&object)
							.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
						let key = crate::clean::Key::AccountObject {
							account,
							object,
							partition,
							touched_at,
						};
						(input, key)
					},
					crate::clean::ItemKind::AccountProcess => {
						let (input, account): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let (input, process): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let account = tg::Id::from_slice(&account).map_err(|_| {
							fdbt::PackError::Message("invalid usage account".into())
						})?;
						let account =
							tangram_index::usage::Account::try_from(account).map_err(|_| {
								fdbt::PackError::Message("invalid usage account".into())
							})?;
						let process = tg::process::Id::from_slice(&process)
							.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
						let key = crate::clean::Key::AccountProcess {
							account,
							partition,
							process,
							touched_at,
						};
						(input, key)
					},
					crate::clean::ItemKind::Checkout => {
						let (input, id): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let id = tg::Id::from_slice(&id)
							.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
						let key = crate::clean::Key::Checkout {
							id,
							partition,
							touched_at,
						};
						(input, key)
					},
					crate::clean::ItemKind::Object => {
						let (input, id): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let id = tg::object::Id::from_slice(&id)
							.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
						let key = crate::clean::Key::Object {
							id,
							partition,
							touched_at,
						};
						(input, key)
					},
					crate::clean::ItemKind::Process => {
						let (input, id): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let id = tg::process::Id::from_slice(&id)
							.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
						let key = crate::clean::Key::Process {
							id,
							partition,
							touched_at,
						};
						(input, key)
					},
					crate::clean::ItemKind::Sandbox => {
						let (input, id): (_, Vec<u8>) =
							fdbt::TupleUnpack::unpack(input, tuple_depth)?;
						let id = tg::sandbox::Id::from_slice(&id)
							.map_err(|_| fdbt::PackError::Message("invalid sandbox id".into()))?;
						let key = crate::clean::Key::Sandbox {
							id,
							partition,
							touched_at,
						};
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
					Key::LogCompaction(crate::log::Key::Identity(process)),
				))
			},

			Kind::LogCompactionVersion => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, version) = fdbt::Versionstamp::unpack(input, tuple_depth)?;
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::LogCompaction(crate::log::Key::Version {
					partition,
					process,
					version,
				});
				Ok((input, key))
			},

			Kind::UsageUpdatePropagatedVersion | Kind::UsageUpdatePutVersion => {
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
				let (input, account): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let account = tg::Id::from_slice(&account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let account = tangram_index::usage::Account::try_from(account)
					.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
				let key = match kind {
					Kind::UsageUpdatePropagatedVersion => {
						crate::update::Key::UsageUpdatePropagatedVersion { account, id }
					},
					_ => crate::update::Key::UsageUpdatePutVersion { account, id },
				};
				Ok((input, Key::Update(key)))
			},

			Kind::PermissionUpdatePropagatedVersion
			| Kind::StorageAndMetadataUpdatePropagatedVersion => {
				let update_kind = match kind {
					Kind::PermissionUpdatePropagatedVersion => {
						tangram_index::update::Kind::Permission
					},
					Kind::StorageAndMetadataUpdatePropagatedVersion => {
						tangram_index::update::Kind::StorageAndMetadata
					},
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
				let key = Key::Update(crate::update::Key::PropagatedVersion { id, kind });
				Ok((input, key))
			},

			Kind::PermissionUpdate | Kind::StorageAndMetadataUpdate | Kind::UsageUpdate => {
				let update_kind = match kind {
					Kind::PermissionUpdate => tangram_index::update::Kind::Permission,
					Kind::StorageAndMetadataUpdate => {
						tangram_index::update::Kind::StorageAndMetadata
					},
					Kind::UsageUpdate => tangram_index::update::Kind::Usage,
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
				Ok((input, Key::Update(crate::update::Key::Update { id, kind })))
			},

			Kind::PermissionUpdateClean
			| Kind::PermissionUpdateVersion
			| Kind::StorageAndMetadataUpdateClean
			| Kind::StorageAndMetadataUpdateVersion
			| Kind::UsageUpdateVersion => {
				let clean = matches!(
					kind,
					Kind::PermissionUpdateClean | Kind::StorageAndMetadataUpdateClean
				);
				let update_kind = match kind {
					Kind::PermissionUpdateClean | Kind::PermissionUpdateVersion => {
						tangram_index::update::Kind::Permission
					},
					Kind::StorageAndMetadataUpdateClean | Kind::StorageAndMetadataUpdateVersion => {
						tangram_index::update::Kind::StorageAndMetadata
					},
					Kind::UsageUpdateVersion => tangram_index::update::Kind::Usage,
					_ => unreachable!(),
				};
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
				let (input, kind) = unpack_update_kind(input, tuple_depth, update_kind)?;
				let key = if clean {
					crate::update::Key::Clean {
						id,
						kind,
						partition,
						version,
					}
				} else {
					crate::update::Key::UpdateVersion {
						id,
						kind,
						partition,
						version,
					}
				};
				let key = Key::Update(key);
				Ok((input, key))
			},
		}
	}
}

fn pack_update_kind<W: std::io::Write>(
	w: &mut W,
	tuple_depth: fdbt::TupleDepth,
	kind: &crate::update::Kind,
) -> std::io::Result<fdbt::VersionstampOffset> {
	match kind {
		crate::update::Kind::Permission(subject) => subject.to_string().pack(w, tuple_depth),
		crate::update::Kind::StorageAndMetadata => ().pack(w, tuple_depth),
		crate::update::Kind::Usage(kind) => match kind {
			crate::update::UsageKind::Clean(account) => {
				let mut offset = 1i32.pack(w, tuple_depth)?;
				offset += account.id().to_bytes().as_ref().pack(w, tuple_depth)?;
				Ok(offset)
			},
			crate::update::UsageKind::CleanAll => 2i32.pack(w, tuple_depth),
			crate::update::UsageKind::Propagate {
				account,
				touched_at,
			} => {
				let mut offset = 3i32.pack(w, tuple_depth)?;
				offset += account.id().to_bytes().as_ref().pack(w, tuple_depth)?;
				offset += touched_at.pack(w, tuple_depth)?;
				Ok(offset)
			},
			crate::update::UsageKind::Put {
				account,
				permissions,
				touched_at,
			} => {
				let mut offset = 0i32.pack(w, tuple_depth)?;
				offset += account.id().to_bytes().as_ref().pack(w, tuple_depth)?;
				let permissions =
					tangram_serialize::to_vec(permissions).map_err(std::io::Error::other)?;
				offset += permissions.as_slice().pack(w, tuple_depth)?;
				offset += touched_at.pack(w, tuple_depth)?;
				Ok(offset)
			},
		},
	}
}

fn unpack_update_kind(
	input: &[u8],
	tuple_depth: fdbt::TupleDepth,
	kind: tangram_index::update::Kind,
) -> Result<(&[u8], crate::update::Kind), fdbt::PackError> {
	match kind {
		tangram_index::update::Kind::Permission => {
			let (input, subject): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
			let subject = subject
				.parse()
				.map_err(|_| fdbt::PackError::Message("invalid verification subject".into()))?;
			Ok((input, crate::update::Kind::Permission(subject)))
		},
		tangram_index::update::Kind::StorageAndMetadata => {
			Ok((input, crate::update::Kind::StorageAndMetadata))
		},
		tangram_index::update::Kind::Usage => {
			let (input, kind): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
			let (input, kind) = match kind {
				0 | 3 => {
					let (input, account): (_, Vec<u8>) =
						fdbt::TupleUnpack::unpack(input, tuple_depth)?;
					let account = tg::Id::from_slice(&account)
						.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
					let account = tangram_index::usage::Account::try_from(account)
						.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
					match kind {
						0 => {
							let (input, bytes): (_, Vec<u8>) =
								fdbt::TupleUnpack::unpack(input, tuple_depth)?;
							let permissions =
								tangram_serialize::from_slice(&bytes).map_err(|_| {
									fdbt::PackError::Message("invalid storage permissions".into())
								})?;
							let (input, touched_at): (_, i64) =
								fdbt::TupleUnpack::unpack(input, tuple_depth)?;
							(
								input,
								crate::update::UsageKind::Put {
									account,
									permissions,
									touched_at,
								},
							)
						},
						3 => {
							let (input, touched_at): (_, i64) =
								fdbt::TupleUnpack::unpack(input, tuple_depth)?;
							(
								input,
								crate::update::UsageKind::Propagate {
									account,
									touched_at,
								},
							)
						},
						_ => unreachable!(),
					}
				},
				1 => {
					let (input, account): (_, Vec<u8>) =
						fdbt::TupleUnpack::unpack(input, tuple_depth)?;
					let account = tg::Id::from_slice(&account)
						.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
					let account = tangram_index::usage::Account::try_from(account)
						.map_err(|_| fdbt::PackError::Message("invalid usage account".into()))?;
					(input, crate::update::UsageKind::Clean(account))
				},
				2 => (input, crate::update::UsageKind::CleanAll),
				_ => {
					return Err(fdbt::PackError::Message("invalid usage update kind".into()));
				},
			};
			Ok((input, crate::update::Kind::Usage(kind)))
		},
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
