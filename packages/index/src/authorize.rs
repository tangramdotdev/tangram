use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub permissions: Permissions,
	pub resource: tg::grant::Resource,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub permissions: Permissions,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Permissions {
	Resource(ResourcePermissions),
	Object(crate::object::Permissions),
	Process(crate::process::Permissions),
}

bitflags::bitflags! {
	#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
	pub struct ResourcePermissions: u8 {
		const READ = 1 << 0;
		const WRITE = 1 << 1;
		const ADMIN = 1 << 2;
	}
}

/// Validate that the permission is coherent with the resource kind.
pub fn validate(resource: &tg::Id, permission: tg::grant::Permission) -> tg::Result<()> {
	let valid = match permission {
		tg::grant::Permission::Admin
		| tg::grant::Permission::Read
		| tg::grant::Permission::Write => {
			matches!(
				resource.kind(),
				tg::id::Kind::User
					| tg::id::Kind::Group
					| tg::id::Kind::Organization
					| tg::id::Kind::Tag
			)
		},
		tg::grant::Permission::Object(_) => tg::object::Id::try_from(resource.clone()).is_ok(),
		tg::grant::Permission::Process(_) => resource.kind() == tg::id::Kind::Process,
	};
	if !valid {
		return Err(tg::error!(%resource, %permission, "invalid permission for the resource"));
	}
	Ok(())
}

impl Permissions {
	#[must_use]
	pub fn from_grant_permission(permission: tg::grant::Permission) -> Self {
		match permission {
			tg::grant::Permission::Admin => Self::Resource(ResourcePermissions::ADMIN),
			tg::grant::Permission::Read => Self::Resource(ResourcePermissions::READ),
			tg::grant::Permission::Write => Self::Resource(ResourcePermissions::WRITE),
			tg::grant::Permission::Object(permission) => Self::Object(
				crate::object::Permissions::from_grant_permission(permission),
			),
			tg::grant::Permission::Process(permission) => Self::Process(
				crate::process::Permissions::from_grant_permission(permission),
			),
		}
	}

	pub fn validate(self, resource: &tg::Id) -> tg::Result<()> {
		for (permission, _) in self.entries() {
			validate(resource, permission)?;
		}
		Ok(())
	}

	#[must_use]
	pub fn contains(self, other: Self) -> bool {
		match (self, other) {
			(Self::Resource(this), Self::Resource(other)) => this.contains(other),
			(Self::Object(this), Self::Object(other)) => this.contains(other),
			(Self::Process(this), Self::Process(other)) => this.contains(other),
			_ => false,
		}
	}

	#[must_use]
	pub fn contains_grant_permission(self, permission: tg::grant::Permission) -> bool {
		self.contains(Self::from_grant_permission(permission))
	}

	#[must_use]
	pub fn is_empty(self) -> bool {
		match self {
			Self::Resource(permissions) => permissions.is_empty(),
			Self::Object(permissions) => permissions.is_empty(),
			Self::Process(permissions) => permissions.is_empty(),
		}
	}

	#[must_use]
	pub fn empty_like(self) -> Self {
		match self {
			Self::Resource(_) => Self::Resource(ResourcePermissions::empty()),
			Self::Object(_) => Self::Object(crate::object::Permissions::empty()),
			Self::Process(_) => Self::Process(crate::process::Permissions::empty()),
		}
	}

	pub fn insert(&mut self, other: Self) {
		match (self, other) {
			(Self::Resource(this), Self::Resource(other)) => this.insert(other),
			(Self::Object(this), Self::Object(other)) => this.insert(other),
			(Self::Process(this), Self::Process(other)) => this.insert(other),
			_ => {},
		}
	}

	pub fn entries(self) -> impl Iterator<Item = (tg::grant::Permission, Self)> {
		let entries = match self {
			Self::Resource(permissions) => [
				permissions.contains(ResourcePermissions::READ).then_some((
					tg::grant::Permission::Read,
					Self::Resource(ResourcePermissions::READ),
				)),
				permissions.contains(ResourcePermissions::WRITE).then_some((
					tg::grant::Permission::Write,
					Self::Resource(ResourcePermissions::WRITE),
				)),
				permissions.contains(ResourcePermissions::ADMIN).then_some((
					tg::grant::Permission::Admin,
					Self::Resource(ResourcePermissions::ADMIN),
				)),
				None,
				None,
				None,
				None,
				None,
				None,
				None,
			],
			Self::Object(permissions) => [
				permissions
					.contains(crate::object::Permissions::NODE)
					.then_some((
						tg::grant::Permission::Object(
							tg::grant::permission::object::Permission::Node,
						),
						Self::Object(crate::object::Permissions::NODE),
					)),
				permissions
					.contains(crate::object::Permissions::SUBTREE)
					.then_some((
						tg::grant::Permission::Object(
							tg::grant::permission::object::Permission::Subtree,
						),
						Self::Object(crate::object::Permissions::SUBTREE),
					)),
				None,
				None,
				None,
				None,
				None,
				None,
				None,
				None,
			],
			Self::Process(permissions) => [
				Self::process_entry(
					permissions,
					tg::grant::permission::process::Permission::Node,
				),
				Self::process_entry(
					permissions,
					tg::grant::permission::process::Permission::NodeCommand,
				),
				Self::process_entry(
					permissions,
					tg::grant::permission::process::Permission::NodeError,
				),
				Self::process_entry(
					permissions,
					tg::grant::permission::process::Permission::NodeLog,
				),
				Self::process_entry(
					permissions,
					tg::grant::permission::process::Permission::NodeOutput,
				),
				Self::process_entry(
					permissions,
					tg::grant::permission::process::Permission::Subtree,
				),
				Self::process_entry(
					permissions,
					tg::grant::permission::process::Permission::SubtreeCommand,
				),
				Self::process_entry(
					permissions,
					tg::grant::permission::process::Permission::SubtreeError,
				),
				Self::process_entry(
					permissions,
					tg::grant::permission::process::Permission::SubtreeLog,
				),
				Self::process_entry(
					permissions,
					tg::grant::permission::process::Permission::SubtreeOutput,
				),
			],
		};
		entries.into_iter().flatten()
	}

	fn process_entry(
		permissions: crate::process::Permissions,
		permission: tg::grant::permission::process::Permission,
	) -> Option<(tg::grant::Permission, Self)> {
		let permissions_ = crate::process::Permissions::from_grant_permission(permission);
		permissions.contains(permissions_).then_some((
			tg::grant::Permission::Process(permission),
			Self::Process(permissions_),
		))
	}
}
